package tui

import "fmt"

// ContextSwitchError asks the CLI host to close the current transport and
// restart the stateless TUI on another local routing context.
type ContextSwitchError struct{ Name string }

func (e *ContextSwitchError) Error() string { return fmt.Sprintf("switch to context %q", e.Name) }
