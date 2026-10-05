# First cache hit: the demo crate is built cold off screen, then the same
# commit is built in a second worktree with an empty target directory.
# See prepare.sh for the workspace; KACHE_DEMO_ROOT selects where it lives.

Output ../demo.gif
Output ../demo.webm

Set Shell "bash"
Set FontSize 16
Set Width 1000
Set Height 360
Set Theme "Catppuccin Mocha"
Set Padding 16
Set TypingSpeed 30ms
Set Framerate 24

Hide
Type "source env.sh"
Enter
Type `cd "$KACHE_DEMO_ROOT/demo-app" && cargo build --locked >/dev/null 2>&1 && kache stats >/dev/null 2>&1 && clear`
Enter
Wait
Show

Type "# the same commit in a second worktree, empty target directory"
Enter
Sleep 800ms
Type "git worktree add ../demo-app-2 HEAD && cd ../demo-app-2"
Enter
Wait
Sleep 800ms
Type "KACHE_PROGRESS=hits cargo build --locked"
Enter
Wait
Sleep 2500ms
Type "clear && kache stats --last-build | head -6"
Enter
Wait
Sleep 6s
