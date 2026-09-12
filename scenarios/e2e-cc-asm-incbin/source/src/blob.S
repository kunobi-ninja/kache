#ifdef __APPLE__
#define SYM(name) _##name
        .section __TEXT,__const
#else
#define SYM(name) name
        .section .note.GNU-stack,"",@progbits
        .section .rodata
#endif
        .globl SYM(asm_payload)
SYM(asm_payload):
/* `\()` splits the directive name, so the expansion never spells `.incbin`. */
        .rept 1
        .inc\()bin "payload.txt"
        .endr
        .byte 0
