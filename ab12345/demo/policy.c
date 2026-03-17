#include <stdio.h>
#include <stdlib.h>
#include <string.h>

char state[STATE_SIZE + 2];
char action[ACTION_SIZE + 2];

int main(int argc, char *argv[])
{
    // Process command-line arguments.
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <id>\n", argv[0]);
        return 1;
    }
    int id = atoi(argv[1]);
    fprintf(stderr, "Policy ID=%d STATE_SIZE=%d, ACTION_SIZE=%d\n", id, (int)STATE_SIZE, (int)ACTION_SIZE);

    // Set fixed action.
    memset(action, 'a', ACTION_SIZE);
    action[ACTION_SIZE] = '\n';
    action[ACTION_SIZE + 1] = '\0';

    // Read states and output fixed action, until EOF or error.
    while (fgets(state, sizeof state, stdin) != NULL) {
        state[STATE_SIZE] = '\0';
        fprintf(stderr, "Policy got state: '%s'\n", state);

        if (fputs(action, stdout) < 0 || fflush(stdout) != 0) {
            perror("fputs");
            return 1;
        }
    }
    if (ferror(stdin)) {
        perror("fgets");
        return 1;
    }

    return 0;
}