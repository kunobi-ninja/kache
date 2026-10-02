# kache clean --dry-run: the target directories Kache tracks, what each
# frees, and what a clean would remove. Runs after demo.tape.

Output ../clean.gif
Output ../clean.webm

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
Type `cd "$KACHE_DEMO_ROOT" && clear`
Enter
Wait
Show

Type "kache clean --dry-run"
Enter
Sleep 7s
