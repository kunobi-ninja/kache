# kache clean showcase. Runs after demo.tape + monitor.tape, with
# project-a duplicated to project-b/c/d via `cp -a` (so hardlinks to
# kache's store are preserved, and `kache clean` reports a meaningful
# cached percentage per directory). Same canvas as the other GIFs.

Output clean.gif

Set FontSize 20
Set Width 1000
Set Height 640
Set Theme "Catppuccin Mocha"
Set Padding 16
Set TypingSpeed 30ms

# Banner
Type "# kache clean — find target/ dirs, see what's cached"
Sleep 1500ms
Ctrl+U

# Move to the workspace root (siblings: project-a, project-b, project-c, project-d)
# and launch the clean TUI.
Type "cd /workspace && kache clean"
Sleep 300ms
Enter
Sleep 9s

# Quit without deleting anything.
Type "q"
Sleep 800ms
