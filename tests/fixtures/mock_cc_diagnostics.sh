#!/bin/sh
# Echo driver flags into the cc1 dump so a cold probe exposes diagnostic leaks.
case "$1" in
  --version) printf 'fake clang 1.0\n' ;;
  -###)
    shift
    printf '"clang" "-cc1"' >&2
    for arg do printf ' "%s"' "$arg" >&2; done
    printf '\n' >&2
    ;;
  *) printf 'preprocessed unit\n' ;;
esac
