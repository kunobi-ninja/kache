#!/bin/sh

if [ "${LC_ALL:-}" != "C" ]; then
    printf 'expected LC_ALL=C, got %s\n' "${LC_ALL:-<unset>}" >&2
    exit 91
fi

case "${1:-}" in
    --version)
        printf '%s\n' 'mock-cc 1.0'
        ;;
    -###)
        printf '%s\n' ' /usr/lib/gcc/cc1 -quiet -O2 foo.c -o foo.s' >&2
        ;;
    -E)
        printf '%s\n' 'KACHE_PROBE_GNU'
        ;;
esac
