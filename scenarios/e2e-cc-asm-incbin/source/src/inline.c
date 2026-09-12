/* The same payload through inline assembly, the way incbin-style C headers
   embed files. The directive name is split across two string literals, which
   the compiler joins only after preprocessing. */
#ifdef __APPLE__
#define PAYLOAD_SECTION ".section __TEXT,__const\n"
#define PAYLOAD_SYMBOL "_c_payload"
#else
#define PAYLOAD_SECTION ".section .rodata\n"
#define PAYLOAD_SYMBOL "c_payload"
#endif

__asm__(PAYLOAD_SECTION ".globl " PAYLOAD_SYMBOL "\n" PAYLOAD_SYMBOL ":\n"
        ".inc" "bin \"payload.txt\"\n"
        ".byte 0\n"
        ".text\n");
