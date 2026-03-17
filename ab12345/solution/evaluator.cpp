#include <csignal>
#include <fcntl.h>
#include <iostream>
#include <list>
#include <map>
#include <queue>
#include <sys/wait.h>
#include <vector>
#include <string>

struct ProcessChannels {
    pid_t pid;
    int fileDescriptorToChild;
    int fileDescriptorFromChild;
};
enum class State {
    RUNNING,
    WAITING_FOR_POLICY,
    READY_TO_SEND,
    WAITING_FOR_ACTION,
    SENDING_TO_ENVIRONMENT,
    FINISHED,
    WAITING_FOR_EXIT
};

struct Environment {
    int testId;
    int assignedPolicyId;
    pid_t pid;
    int fdRead;
    int fdWrite;
    int bytesProcessed = 0;
    std::string testName;
    std::string readBuffer;
    State state = State::RUNNING;
};

struct Policy {
    int id;
    pid_t pid;
    int fdToWrite;
    int fdToRead;
    bool isBusy;
    std::string readBuffer;
};

extern "C" {
    [[noreturn]] void fatal(const char* fmt, ...);
    [[noreturn]] void syserr(const char* fmt, ...);
}

int currentCalls = 0;   // Counts active processes.
std::vector<Policy> policies;
std::string inputBuffer; // Collects characters from stdin.
std::queue<std::string> awaitingTests;
std::list<Environment> runningEnvironments;
int environmentProcessesCount = 0;
int activePolicyProcesses = 0;
bool wereAllTestsPassed = true;
volatile sig_atomic_t stop_requested = false; // Flag to indicate if SIGINT has been received.
char const *PolicyPath = nullptr;
char const *envPath = nullptr;
int maxConcurrentPolicyCalls = 0;
int maxConcurrentCalls = 0;
int maxActiveEnvironments = 0;
std::vector<const char*> extraArgs; // Stores remaining command-line arguments.
int testCounter = 0;
int nextToPrint = 0;
bool sdtdinReachedEOF = false;
std::map<int, std::string> resultsBuffer;

/* Launches a subprocess with the given path.
 * Creates pipes allowing for communication:
 * - FileDescriptorToChild: Evaluator -> Subprocess
 * - FileDescriptorFromChild: Subprocess -> Evaluator
 */
ProcessChannels launch(const char* path, const std::vector<const char*>& args) {
    int pipe_to[2];     // Pipe: Evaluator -> Subprocess
    int pipe_from[2];   // Pipe: Subprocess -> Evaluator

    // Creating two pipes.
    if (pipe(pipe_to) == -1 || pipe(pipe_from) == -1) {
        syserr("Failed to create pipes for subprocess communication.");
    }

    pid_t pid = fork();
    if (pid == -1) {
        syserr("Failed to fork subprocess.");
    }

    // Child code segment.
    if (pid == 0) {
        if (dup2(pipe_to[0], STDIN_FILENO) == -1) syserr("dup2 stdin");
        if (dup2(pipe_from[1], STDOUT_FILENO) == -1) syserr("dup2 stdout");
        close(pipe_to[0]);
        close(pipe_from[1]);
        close(pipe_to[1]);
        close(pipe_from[0]);
        // Executing the subprocess.
        execvp(path, const_cast<char* const*>(args.data()));
        syserr("Failed to execute subprocess.");
    }
    // Parent code segment.
    close(pipe_to[0]);
    close(pipe_from[1]);
    fcntl(pipe_to[1], F_SETFD, FD_CLOEXEC);
    fcntl(pipe_from[0], F_SETFD, FD_CLOEXEC);
    // Gets the file descriptor flags.
    int flagsFrom = fcntl(pipe_from[0], F_GETFL);
    int flagsTo = fcntl(pipe_to[1], F_GETFL);
    if (flagsFrom == -1 || flagsTo == -1) syserr("fcntl F_GETFL");
    // Adds non-blocking flag.
    if (fcntl(pipe_from[0], F_SETFL, flagsFrom | O_NONBLOCK) == -1) syserr("fcntl F_SETFL");
    if (fcntl(pipe_to[1], F_SETFL, flagsTo | O_NONBLOCK) == -1) syserr("fcntl F_SETFL");
    return ProcessChannels{pid, pipe_to[1], pipe_from[0]};
}

void spawnNewPolicyProcess() {
    // Getting the id and appending extraArgs to a temporary vector for storing arguments.
    int i = policies.size();
    std::string policyId = std::to_string(i);
    std::vector<const char*> policyArgs = {PolicyPath, policyId.c_str()};
    policyArgs.insert(policyArgs.end(), extraArgs.begin(), extraArgs.end());
    policyArgs.push_back(nullptr);
    // Launching the Policy process and adding it to a vector.
    ProcessChannels channels = launch(PolicyPath, policyArgs);
    policies.push_back({.id = i, .pid=channels.pid, .fdToWrite=channels.fileDescriptorToChild,
    .fdToRead=channels.fileDescriptorFromChild, .isBusy=false, .readBuffer=""});
}

void readTestNameFromStdin() {
    char buffer[4096]; // Buffer for a singular read.
    const ssize_t r = read(STDIN_FILENO, buffer, sizeof(buffer));
    if (r > 0) {
        inputBuffer.append(buffer, r);
        while (inputBuffer.length() >= static_cast<size_t>(NAME_SIZE) + 1) {
            std::string test_name = inputBuffer.substr(0, NAME_SIZE);

            // Checking if the format is proper.
            if (inputBuffer[NAME_SIZE] == '\n') {
                awaitingTests.push(test_name); // Adding to awaitingTests queue.
            }

            // Clearing the inputBuffer.
            inputBuffer.erase(0, NAME_SIZE + 1);
        }
    }
    else if (r == 0) sdtdinReachedEOF = true;
}

void createEnvironmentProcesses() {
    while (!stop_requested && environmentProcessesCount < maxActiveEnvironments &&
        !awaitingTests.empty() && currentCalls < maxConcurrentCalls) {
        std::string test_name = awaitingTests.front();
        // Appending test_name and extra args into a vector.
        std::vector<const char*> args = {envPath, test_name.c_str()};
        args.insert(args.end(), extraArgs.begin(), extraArgs.end());
        args.push_back(nullptr);
        awaitingTests.pop();
        // Launching the environment.
        ProcessChannels p = launch(envPath, args);
        environmentProcessesCount++;
        currentCalls++;
        // Creating an environment class and adding to a vector.
        Environment e = Environment{.testId = testCounter++, .assignedPolicyId = -1, .pid=p.pid, .fdRead=p.fileDescriptorFromChild,
            .fdWrite=p.fileDescriptorToChild, .testName=test_name, .readBuffer="", .state=State::RUNNING };
        runningEnvironments.push_back(e);
    }
}

void processRunningEnvironments() {
    bool environmentErased = false;
    for (auto it = runningEnvironments.begin(); it != runningEnvironments.end();) {
        environmentErased = false;
        switch (it->state) {

            // Reading data from the environment process.
            case State::RUNNING: {
                int targetSize = STATE_SIZE + 1;
                it->readBuffer.resize(targetSize);
                ssize_t r = read(it->fdRead, it->readBuffer.data() + it->bytesProcessed, targetSize - it->bytesProcessed);
                if (r > 0) {
                    it->bytesProcessed += r;
                    if (it->bytesProcessed == targetSize) {
                        currentCalls--;
                        it->bytesProcessed = 0;
                        if (it->readBuffer[0] == 'T') {
                            it->state = State::FINISHED;
                        }
                        else
                            it->state = State::WAITING_FOR_POLICY;
                    }
                }
                else if (r == 0) {
                    if (it->bytesProcessed < targetSize) {
                        wereAllTestsPassed = false;
                        syserr("read environment: unexpected EOF");
                    }
                    it->state = State::FINISHED;
                }
                else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                    wereAllTestsPassed = false;
                    it->state = State::FINISHED;
                    syserr("read environment");
                }
                break;
            }

            // Looping over policies to check whether there is a free one to allocate for this environment.
            case State::WAITING_FOR_POLICY: {
                // There isn't a set limit on the number of policy processes, however,
                // creating more than maxConcurrentPolicyCalls is unnecessary.
                if (currentCalls < maxConcurrentCalls && activePolicyProcesses < maxConcurrentPolicyCalls) {
                    for (Policy& p : policies) {
                        if (!p.isBusy) {
                            p.isBusy = true;
                            p.readBuffer.clear();
                            it->assignedPolicyId = p.id;
                            it->bytesProcessed = 0;
                            it->state = State::READY_TO_SEND;
                            activePolicyProcesses++;
                            currentCalls++;
                            break;
                        }
                    }
                    if (it->assignedPolicyId == -1) {
                        spawnNewPolicyProcess();
                        int policyId = policies.size() - 1;
                        policies[policyId].isBusy = true;
                        policies[policyId].readBuffer.clear();
                        it->assignedPolicyId = policyId;
                        currentCalls++;
                        activePolicyProcesses++;
                        it->bytesProcessed = 0;
                        it->state = State::READY_TO_SEND;
                    }
                }
                break;
            }

            // Writing current state to policy.
            case State::READY_TO_SEND: {
                int policyFd = policies[it->assignedPolicyId].fdToWrite;
                int TargetSize = STATE_SIZE + 1;
                const char* dataPtr = it->readBuffer.data() + it->bytesProcessed;
                size_t bytesRemaining = TargetSize - it->bytesProcessed;
                ssize_t w = write(policyFd, dataPtr, bytesRemaining);
                if (w > 0) {
                    it->bytesProcessed += w;
                    if (it->bytesProcessed == TargetSize) {
                        // The policy process becomes active.
                        it->bytesProcessed = 0;
                        it->readBuffer.clear();
                        it->state = State::WAITING_FOR_ACTION;
                    }
                }
                else if (w < 0) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        wereAllTestsPassed = false;
                        it->state = State::FINISHED;
                        syserr("write policy");
                    }
                }
                break;
            }

            // Waiting for a reply from the policy.
            case State::WAITING_FOR_ACTION: {
                int policyFd = policies[it->assignedPolicyId].fdToRead;
                int TARGET_SIZE = ACTION_SIZE + 1;
                it->readBuffer.resize(TARGET_SIZE);
                ssize_t r = read(policyFd, it->readBuffer.data() + it->bytesProcessed, TARGET_SIZE - it->bytesProcessed);
                if (r > 0) {
                    it->bytesProcessed += r;
                    if (it->bytesProcessed == TARGET_SIZE) {
                        // The policy process becomes inactive.
                        it->bytesProcessed = 0;
                        policies[it->assignedPolicyId].isBusy = false;
                        currentCalls--;
                        activePolicyProcesses--;
                        it->state = State::SENDING_TO_ENVIRONMENT;
                    }
                }
                else if (r == 0) {
                    if (it->bytesProcessed < TARGET_SIZE) {
                        wereAllTestsPassed = false;
                        it->state = State::FINISHED;
                        syserr("read policy: unexpected EOF");
                    }
                }
                else if (r < 0) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        wereAllTestsPassed = false;
                        it->state = State::FINISHED;
                        syserr("read policy");
                    }
                }
                break;
            }

            // Sending the policy reply to the environment.
            case State::SENDING_TO_ENVIRONMENT: {
                // Checking if an environment process can be active within given limits.
                if (it->bytesProcessed == 0 && currentCalls >= maxConcurrentCalls) {
                    break;
                }
                int targetSize = ACTION_SIZE + 1;
                const char* dataPtr = it->readBuffer.data() + it->bytesProcessed;
                size_t bytesRemaining = targetSize - it->bytesProcessed;
                ssize_t w = write(it->fdWrite, dataPtr, bytesRemaining);
                if (w > 0) {
                    it->bytesProcessed += w;
                    if (it->bytesProcessed == targetSize) {
                        // The environment process becomes active.
                        currentCalls++;
                        it->bytesProcessed = 0;
                        it->readBuffer.clear();
                        it->state = State::RUNNING;
                    }
                }
                else if (w < 0) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        wereAllTestsPassed = false;
                        it->state = State::FINISHED;
                        syserr("write environment");
                    }
                }
                break;
            }

            case State::FINISHED: {
                std::string finalState = "";
                if (it->readBuffer.length() > 1) {
                    finalState = it->readBuffer.substr(0, STATE_SIZE);
                }
                resultsBuffer[it->testId] = it->testName + " " + finalState;
                while (resultsBuffer.count(nextToPrint) > 0) {
                    std::cout << resultsBuffer.at(nextToPrint) << std::endl;
                    resultsBuffer.erase(nextToPrint++);
                }
                it->state = State::WAITING_FOR_EXIT;
                close(it->fdRead);
                close(it->fdWrite);
                [[fallthrough]];
            }

            case State::WAITING_FOR_EXIT: {
                pid_t result = waitpid(it->pid, nullptr, WNOHANG);
                if (result > 0 || result == -1) {
                    it = runningEnvironments.erase(it);
                    environmentErased = true;
                    environmentProcessesCount--;
                }
                break;
            }
        }
        if (!environmentErased) ++it;
    }
}

// Sends SIGINT to all active Policy and Environment processes. Closes all open file descriptors.
void cleanup() {
    for (auto& policy : policies) {
        kill(policy.pid, SIGINT);
    }
    for (auto& env : runningEnvironments) {
        kill(env.pid, SIGINT);
    }
    // Close descriptors and wait for processes to finish.
    for (auto& p : policies) {
        close(p.fdToRead);
        close(p.fdToWrite);
        waitpid(p.pid, nullptr, 0);
    }
    for (auto& e : runningEnvironments) {
        close(e.fdRead);
        close(e.fdWrite);
        waitpid(e.pid, nullptr, 0);
    }
}

void handle_sigint(int signum) {
    stop_requested = true;
}

// Replaces SIGINT handling with the above function which sets a flag to true.
void setupSIGINTHandling() {
    struct sigaction saInterrupt;
    saInterrupt.sa_handler = handle_sigint;
    sigemptyset(&saInterrupt.sa_mask);
    saInterrupt.sa_flags = 0;
    sigaction(SIGINT, &saInterrupt, nullptr);
}


int main(int argc, char *argv[]) {

    setupSIGINTHandling();

    if (argc < 6) {
        fatal("Invalid number of arguments provided. Expected at least 6 arguments.");
    }
    // Reading command-line arguments.
    PolicyPath = argv[1];
    envPath = argv[2];
    maxConcurrentPolicyCalls = std::stoi(argv[3]);
    maxConcurrentCalls = std::stoi(argv[4]);
    maxActiveEnvironments = std::stoi(argv[5]);
    for (int i = 6; i < argc; i++) {
        extraArgs.push_back(argv[i]);
    }

    // Making std::cin non-blocking.
    int flags = fcntl(STDIN_FILENO, F_GETFL, 0);
    fcntl(STDIN_FILENO, F_SETFL, flags | O_NONBLOCK);
    while (true) {
        // Break if SIGINT has been received.
        if (stop_requested) break;
        
        if (!stop_requested && !sdtdinReachedEOF)
            readTestNameFromStdin();

        if (!stop_requested) {
            createEnvironmentProcesses(); // Only if it's within processes limits.
            processRunningEnvironments();
        }

        if (sdtdinReachedEOF && awaitingTests.empty() && runningEnvironments.empty()) {
            break;
        }
    }

    // Kill all open processes and close file descriptors.
    cleanup();
    if (stop_requested) return 2;
    if (!wereAllTestsPassed) return 1;
    return 0;
}