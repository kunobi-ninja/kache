#include <stdio.h>

int a_val(void);
int b_val(void);

int main(void) {
    printf("multi-source ok %d\n", a_val() + b_val());
    return 0;
}
