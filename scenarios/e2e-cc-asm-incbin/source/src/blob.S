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
/* The directive name is only assembled from the macro argument, so the
   expansion never spells `.incbin`. GNU as and LLVM both accept this form. */
        .macro emit op, file
        .\op "\file"
        .endm
        emit incbin, payload.txt
        .byte 0
