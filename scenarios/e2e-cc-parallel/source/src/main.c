#include <stdio.h>
int f00(void);
int f01(void);
int f02(void);
int f03(void);
int f04(void);
int f05(void);
int f06(void);
int f07(void);
int f08(void);
int f09(void);
int f10(void);
int f11(void);
int main(void) {
    int total = 0;
    total += f00();
    total += f01();
    total += f02();
    total += f03();
    total += f04();
    total += f05();
    total += f06();
    total += f07();
    total += f08();
    total += f09();
    total += f10();
    total += f11();
    printf("cc-parallel ok: %d\n", total);
    return 0;
}
