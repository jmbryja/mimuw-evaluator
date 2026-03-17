#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

char state[STATE_SIZE + 2];
char action[ACTION_SIZE + 2];

int main(int argc, char *argv[])
{
    // Process command-line arguments.
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <testName>\n", argv[0]);
        return 1;
    }
    const char *testName = argv[1];
    fprintf(stderr, "Env testName=%s STATE_SIZE=%d, ACTION_SIZE=%d\n", testName, (int)STATE_SIZE, (int)ACTION_SIZE);

    // Set initial state.
    memset(state, 'Z', STATE_SIZE);
    state[STATE_SIZE] = '\n';
    state[STATE_SIZE + 1] = '\0';

    state[0] = testName[0];

    while (true) {
        // Print state.
        if (state[0] < 'A' || state[0] > 'T') {
            fprintf(stderr, "Invalid state for this env, first char: '%c'\n", state[0]);
            return 1;
        }
        if (fputs(state, stdout) < 0 || fflush(stdout) != 0) {
            perror("fputs");
            return 1;
        }
        if (state[0] == 'T') {
            break;
        }

        // Read action.
        if (fgets(action, sizeof action, stdin) == NULL) {
            if (ferror(stdin)) {
                perror("fgets");
                return 1;
            } else {
                fprintf(stderr, "Env expected action\n");
                return 1;
            }
        }
        action[ACTION_SIZE] = '\0';
        fprintf(stderr, "Env got action: '%s'\n", action);

        // Update state based on action.
        state[0]++;
    }

    if (ferror(stdin)) {
        perror("fgets");
        return 1;
    }

    return 0;
}