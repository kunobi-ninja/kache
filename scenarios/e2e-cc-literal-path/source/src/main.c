#include <stdio.h>
#include <string.h>
#include <unistd.h>

const char *data_dir(void);

/* The harness runs this from the checkout that built it. data_dir() must name
   that checkout: a restored object from another path would name the other one. */
int main(void) {
    char cwd[4096];
    if (!getcwd(cwd, sizeof cwd)) {
        return 2;
    }
    const char *dir = data_dir();
    size_t len = strlen(cwd);
    if (strncmp(dir, cwd, len) != 0 || strcmp(dir + len, "/data") != 0) {
        printf("stale path: %s (checkout %s)\n", dir, cwd);
        return 1;
    }
    printf("app ok\n");
    return 0;
}
