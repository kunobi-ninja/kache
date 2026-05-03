# kache monitor showcase. Runs after demo.tape, so the cache is already
# populated. Wider terminal + bigger font so the TUI's multi-column
# tables (Store, Projects) render with breathing room.

Output monitor.gif

Set FontSize 18
Set Width 1400
Set Height 900
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

# Build tab (default) — header stats, live build feed, hit-rate chart
Sleep 5s

# Projects tab — per-project breakdown
Type "2"
Sleep 4s

# Store tab — every cached entry by blake3 key
Type "3"
Sleep 5s

# Transfer tab — uploads / downloads (empty without a remote)
Type "4"
Sleep 3s

# Back to Build tab to close on the headline view
Type "1"
Sleep 3s

# Quit
Type "q"
Sleep 800ms
