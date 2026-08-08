Require orchigram

Set Shell zsh
Set Width 1600
Set Height 900
Set FontSize 17
Set FontFamily "SF Mono"
Set LineHeight 1.15
Set Padding 20
Set TypingSpeed 1ms
Set Theme { "name": "Orchigram", "black": "#0b0f0d", "red": "#f16d70", "green": "#8fdf73", "yellow": "#eab950", "blue": "#80bfff", "magenta": "#c7a0ff", "cyan": "#72d2c5", "white": "#e8e5dd", "brightBlack": "#6f7772", "brightRed": "#ff8a8d", "brightGreen": "#adf58f", "brightYellow": "#ffd16a", "brightBlue": "#9fd0ff", "brightMagenta": "#dbbdff", "brightCyan": "#91e6da", "brightWhite": "#fffdf5", "background": "#080b09", "foreground": "#e8e5dd", "selection": "#243026", "cursor": "#adf58f" }

Type "exec orchigram --socket $ORCHIGRAM_DEMO_SOCKET"
Enter
Sleep 2s

Down 3
Enter
Sleep 1s
Screenshot docs/site/assets/tui-flow.png

Type ":"
Sleep 200ms
Type "runs"
Enter
Down
Enter
Sleep 5s
Screenshot docs/site/assets/tui-run-approval.png

Type ":"
Sleep 200ms
Type "triggers"
Enter
Down
Enter
Sleep 1s
Screenshot docs/site/assets/tui-trigger.png

Escape
Type "q"
