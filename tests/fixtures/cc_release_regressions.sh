#!/usr/bin/env bash
set -euo pipefail

# Usage: cc_release_regressions.sh KACHE CLANG ISSUE [EVIDENCE_DIRECTORY]
kache_binary=$(realpath "${1:?provide the kache binary}")
cc_binary=$(command -v "${2:-clang}")
cc_binary=$(realpath "$cc_binary")
issue=${3:?provide issue 1459, 1460, 1461 or 1462}
repro_root=${4:-$(mktemp -d "${TMPDIR:-/tmp}/kache-cc-regression.XXXXXX")}
repro_root=$(realpath "$repro_root")
export KACHE_CACHE_DIR="$repro_root/cache"
export KACHE_RUNTIME_DIR="$repro_root/cache"
export KACHE_CONFIG="$repro_root/config.toml"
export KACHE_HOST_CONFIG="$repro_root/no-host-config.toml"
export KACHE_DAEMON_PUBLISH=0
export KACHE_MIN_STORE_COMPILE_MS=0
export KACHE_LOG='kache::wrapper=debug'
unset KACHE_BASE_DIR KACHE_ACTIVE KACHE_SOCKET_PATH KACHE_EVENT_ROOT
printf '[cache]\ndaemon_publish = false\n' > "$KACHE_CONFIG"
printf 'Issue #%s evidence: %s\n' "$issue" "$repro_root"
cd "$repro_root"

fail() { printf 'FAIL #%s: %s\n' "$issue" "$*" >&2; exit 1; }
hit() { grep -q 'cc local cache hit' "$1" || fail "expected cache hit in $1"; }
# Kache does not store a compile that starts within one stamp window of a
# write to its inputs: 1 ms on Linux and macOS, 20 ms elsewhere, and about
# two seconds where stamps keep whole seconds.
settle() { sleep 2.1; }

case "$issue" in
1459)
  printf 'int f(void) { return 1; }\n' > a.c
  printf '#include <stdio.h>\nint f(void);\nint main(void) { printf("%%d\\n", f()); }\n' > main.c
  cat > Makefile <<'MAKEFILE'
all: app
a.o: a.c
	$(KACHE_BIN) $(CC_BIN) -c a.c -o a.o
app: a.o main.c
	$(CC_BIN) main.c a.o -o app
MAKEFILE
  build() { make KACHE_BIN="$kache_binary" CC_BIN="$cc_binary" "$@"; }
  settle
  build > initial.log 2>&1
  test "$(./app)" = 1 || fail 'initial binary returned the wrong value'
  sleep 1
  printf 'int f(void) { return 2; }\n' > a.c
  build > edited.log 2>&1
  test "$(./app)" = 2 || fail 'edited binary returned the wrong value'
  sleep 1
  printf 'int f(void) { return 1; }\n' > a.c
  build > restored.log 2>&1
  hit restored.log
  test "$(./app)" = 1 || fail 'Make kept running value 2 after restoring the cached value 1'
  build -q || fail 'Make still considers the restored object stale'
  ;;
1460)
  for tree in A B; do
    mkdir -p "$tree/src/inc" "$tree/obj/sub"
    printf '#define V 1\n' > "$tree/src/inc/h.h"
    printf '#include "h.h"\nint f(void) { return V; }\n' > "$tree/obj/sub/gen.c"
    cat > "$tree/obj/sub/Makefile" <<'MAKEFILE'
gen.o: gen.c
-include gen.o.pp
MAKEFILE
    settle
    (
      cd "$tree/obj/sub"
      KACHE_BASE_DIR="$repro_root/$tree" "$kache_binary" "$cc_binary" \
        -I"$repro_root/$tree/src/inc" -MD -MP -MF gen.o.pp -c gen.c -o gen.o
    ) > "$tree.log" 2>&1
  done
  hit B.log
  # Factor out old restored timestamps to test the depfile independently.
  touch B/obj/sub/gen.o
  sleep 1
  printf '#define V 2\n' > B/src/inc/h.h
  if make -C B/obj/sub -q; then
    fail 'Make ignored the consumer header edit'
  fi
  grep -Fq "$repro_root/B/src/inc/h.h" B/obj/sub/gen.o.pp || fail 'depfile lacks the consumer header'
  if grep -Fq "$repro_root/A/" B/obj/sub/gen.o.pp; then
    fail 'depfile still names the donor checkout'
  fi
  touch B/obj/sub/gen.o
  sleep 1
  printf '#define V 7\n' > A/src/inc/h.h
  make -C B/obj/sub -q || fail 'Make watches a donor-only header edit'
  ;;
1461)
  printf 'int f(void) { return 1; }\n' > a.c
  settle
  "$kache_binary" "$cc_binary" -c a.c -o plain.o > plain.log 2>&1
  "$kache_binary" "$cc_binary" -fcolor-diagnostics -c a.c -o colored.o > colored.log 2>&1
  cmp plain.o colored.o || fail 'diagnostic formatting changed object bytes'
  hit colored.log
  ;;
1462)
  printf 'int f(void) { return 1; }\n' > a.c
  compile=("$kache_binary" "$cc_binary")
  if "$cc_binary" --print-targets | grep -q wasm32; then
    compile+=(--target=wasm32-wasip1)
  fi
  settle
  "${compile[@]}" -c a.c -o a.wasm > first.log 2>&1
  cp a.wasm expected.wasm
  rm a.wasm
  "${compile[@]}" -c a.c -o a.wasm > restored.log 2>&1
  cmp expected.wasm a.wasm || fail 'restored compile output differs'
  hit restored.log
  ;;
*) fail 'unknown issue';;
esac
printf 'PASS #%s: real clang/Make regression\n' "$issue"
