# kache monitor showcase. Runs after demo.tape, so the cache is already
# populated. Same canvas size and font as demo.tape so the two GIFs
# render at a consistent scale when GitHub displays them side by side.
# At 1000×640 the multi-column tabs (Projects/Store/Transfer) get
# cramped, so we focus on the Build tab — the headline dashboard with
# Store/Hit rate/Dedup/Hardlinks all visible.

Output monitor.gif

Set FontSize 20
Set Width 1000
Set Height 640
Set Theme "Catppuccin Mocha"
Set Padding 16
Set TypingSpeed 30ms

# Banner
Type "# kache monitor — live cache dashboard"
Sleep 1500ms
Ctrl+U

# Launch the TUI
Type "kache monitor"
Sleep 300ms
Enter

# Build tab (default) — header stats, live build feed, hit-rate chart.
# Hold long enough that a viewer can read every headline number.
Sleep 9s

# Quit
Type "q"
Sleep 800ms
