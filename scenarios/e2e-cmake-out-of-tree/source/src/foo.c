#include "version.h"
#include "bar.h"
#include <stdio.h>

int main(void) {
    printf("%s (%d)\n", GREETING, bar_value());
    return 0;
}
