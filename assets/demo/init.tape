# kache init: preview the changes, apply them, and let doctor confirm the
# terminal. HOME and CARGO_HOME point into the demo root so nothing on the
# recording machine changes; the login service is skipped for the same reason.

Output ../init.gif
Output ../init.webm

Set Shell "bash"
Set FontSize 16
Set Width 1000
Set Height 640
Set Theme "Catppuccin Mocha"
Set Padding 16
Set TypingSpeed 30ms
Set Framerate 24

Hide
Type "source env.sh"
Enter
Type `export HOME="$KACHE_DEMO_ROOT/home" CARGO_HOME="$KACHE_DEMO_ROOT/cargo-home" SHELL=/bin/zsh && rm -rf "$HOME" "$CARGO_HOME" && mkdir -p "$HOME" "$CARGO_HOME" && cd "$KACHE_DEMO_ROOT" && clear`
Enter
Wait
Show

Type "kache init --check"
Enter
Wait
Sleep 4s
Type "clear && kache init --no-service"
Enter
Sleep 2s
Enter
Sleep 2s
Enter
Sleep 2s
Enter
Wait
Sleep 3s
Type "clear && kache doctor"
Enter
Wait
Sleep 6s
