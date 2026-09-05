#!/bin/sh
# Keep probe and preprocessor output constant so tests can isolate key inputs.
case "$1" in
  --version) printf 'fake gcc 1.0\n' ;;
  -###) exit 0 ;;
  *) printf 'preprocessed unit\n' ;;
esac
