#!/bin/sh

case "${1:-}" in
    --version)
        printf '%s\n' 'fake gcc 1.0'
        ;;
    -###)
        exit 0
        ;;
    *)
        printf '%s\n' 'preprocessed unit'
        ;;
esac
