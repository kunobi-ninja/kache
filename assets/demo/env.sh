# Environment for the recordings. Every tape sources this off screen; prepare.sh
# uses the same root. Override KACHE_DEMO_ROOT to record somewhere else.
case "$(uname)" in
  Darwin) default_root=/Users/Shared/kache-demo ;; # the monitor's project scan skips /private
  *) default_root=/tmp/kache-demo ;;
esac
export KACHE_DEMO_ROOT="${KACHE_DEMO_ROOT:-$default_root}"
export PATH="$KACHE_DEMO_ROOT/bin:$HOME/.cargo/bin:$PATH"
export KACHE_CACHE_DIR="$KACHE_DEMO_ROOT/store"
export KACHE_RUNTIME_DIR="$KACHE_DEMO_ROOT/runtime"
export KACHE_CONFIG="$KACHE_DEMO_ROOT/kache.toml"
export RUSTC_WRAPPER="$KACHE_DEMO_ROOT/bin/kache"
export CARGO_TERM_COLOR=always
