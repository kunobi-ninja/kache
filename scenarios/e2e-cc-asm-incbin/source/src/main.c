#include <stdio.h>
#include <string.h>

extern const char asm_payload[];
extern const char c_payload[];

/* Both payloads come from build/payload.txt through `.incbin`. A cached object
   built in another checkout would carry that checkout's checksum instead. */
int main(void) {
    if (strcmp(asm_payload, EXPECTED) != 0 || strcmp(c_payload, EXPECTED) != 0) {
        printf("stale payload: asm=%s c=%s expected=%s\n", asm_payload, c_payload, EXPECTED);
        return 1;
    }
    printf("app ok\n");
    return 0;
}
