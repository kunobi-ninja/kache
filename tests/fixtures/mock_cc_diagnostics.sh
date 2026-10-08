#!/bin/sh
# Echo driver flags into the cc1 dump so a cold probe exposes diagnostic leaks.
case "$1" in
  --version) printf 'fake clang 1.0\n' ;;
  -###)
    shift
    printf '"clang" "-cc1"' >&2
    source=''
    for arg do
      case "$arg" in
        *.c) source="$arg" ;;
        *) printf ' "%s"' "$arg" >&2 ;;
      esac
    done
    printf ' "%s"\n' "$source" >&2
    ;;
  *) printf 'preprocessed unit\n' ;;
esac
