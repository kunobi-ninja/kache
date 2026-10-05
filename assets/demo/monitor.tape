# kache monitor while a build runs in a third worktree.
# Runs after demo.tape.

Output ../monitor.gif
Output ../monitor.webm

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
Type `cd "$KACHE_DEMO_ROOT/demo-app" && git worktree add ../demo-app-3 HEAD >/dev/null 2>&1; (sleep 3 && cd ../demo-app-3 && cargo build --locked >/dev/null 2>&1) & clear`
Enter
Wait
Show

Type "kache monitor --since 1h"
Enter
Sleep 8s
Type "2"
Sleep 3500ms
Type "3"
Sleep 3500ms
Type "4"
Sleep 3500ms
Type "5"
Sleep 3500ms
Type "q"
Sleep 800ms
