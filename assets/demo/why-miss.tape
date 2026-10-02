# One source edit, one recompile, and kache explain says which key changed.
# Runs after demo.tape (uses its second worktree).

Output ../why-miss.gif
Output ../why-miss.webm

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
Type `cd "$KACHE_DEMO_ROOT/demo-app-2" && clear`
Enter
Wait
Show

Type "perl -pi -e 's/Hash every file/Hash each file/' src/main.rs"
Enter
Sleep 800ms
Type "KACHE_PROGRESS=verbose cargo build --locked"
Enter
Wait
Sleep 2s
Type "clear && kache explain demo_app"
Enter
Wait
Sleep 7s
