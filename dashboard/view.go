package dashboard

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"g-diwakar/distributed-task-queue/internal/job"
)

// Column widths
const (
	colID       = 10
	colType     = 16
	colStatus   = 12
	colPriority = 8
	colAttempts = 4
	colWorker   = 22
)

var (
	styleTitle    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	styleHeader   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("244"))
	styleDivider  = lipgloss.NewStyle().Foreground(lipgloss.Color("238"))
	styleDim      = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	styleError    = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
	styleSelected = lipgloss.NewStyle().Background(lipgloss.Color("237")).Bold(true)
	styleEmpty    = lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Italic(true)

	statusStyles = map[job.Status]lipgloss.Style{
		job.StatusPending:   lipgloss.NewStyle().Foreground(lipgloss.Color("220")), // yellow
		job.StatusRunning:   lipgloss.NewStyle().Foreground(lipgloss.Color("39")),  // blue
		job.StatusCompleted: lipgloss.NewStyle().Foreground(lipgloss.Color("82")),  // green
		job.StatusRetrying:  lipgloss.NewStyle().Foreground(lipgloss.Color("208")), // orange
		job.StatusFailed:    lipgloss.NewStyle().Foreground(lipgloss.Color("196")), // red
		job.StatusDead:      lipgloss.NewStyle().Foreground(lipgloss.Color("124")), // dark red
		job.StatusCancelled: lipgloss.NewStyle().Foreground(lipgloss.Color("240")), // gray
	}
)

func (m Model) View() string {
	if m.showHelp {
		return helpView()
	}

	var b strings.Builder

	// ── Title + filter ──────────────────────────────────────────────────
	filterLabel := "all"
	if m.statusIdx > 0 {
		filterLabel = string(statusCycle[m.statusIdx])
	}
	b.WriteString(styleTitle.Render("Distributed Task Queue"))
	b.WriteString("  ")
	b.WriteString(styleDim.Render(fmt.Sprintf("filter:%-11s", filterLabel)))
	b.WriteString(styleDim.Render("[↑↓] navigate  [f] filter  [r] refresh  [?] help  [q] quit"))
	b.WriteString("\n\n")

	// ── Column headers ───────────────────────────────────────────────────
	b.WriteString(styleHeader.Render(fmt.Sprintf(
		"  %-*s  %-*s  %-*s  %-*s  %-*s  %-*s  %s",
		colID, "ID",
		colType, "TYPE",
		colStatus, "STATUS",
		colPriority, "PRIORITY",
		colAttempts, "ATT",
		colWorker, "WORKER",
		"AGE",
	)))
	b.WriteString("\n")
	b.WriteString(styleDivider.Render(strings.Repeat("─", dividerWidth(m.width))))
	b.WriteString("\n")

	// ── Job rows ─────────────────────────────────────────────────────────
	maxRows := m.height - 7
	if maxRows < 1 {
		maxRows = 1
	}
	visible := m.jobs
	if len(visible) > maxRows {
		visible = visible[:maxRows]
	}

	if len(visible) == 0 {
		b.WriteString(styleEmpty.Render("  no jobs"))
		b.WriteString("\n")
	}

	for i, j := range visible {
		row := fmt.Sprintf(
			"  %-*s  %-*s  %-*s  %-*s  %-*d  %-*s  %s",
			colID, truncate(j.ID, colID),
			colType, truncate(string(j.Type), colType),
			colStatus, string(j.Status),
			colPriority, priorityLabel(j.Priority),
			colAttempts, j.Attempts,
			colWorker, workerLabel(j.WorkerID),
			age(j.CreatedAt),
		)
		if i == m.cursor {
			b.WriteString(styleSelected.Render(row))
		} else if st, ok := statusStyles[j.Status]; ok {
			b.WriteString(st.Render(row))
		} else {
			b.WriteString(row)
		}
		b.WriteString("\n")
	}

	// ── Footer: per-status counts ─────────────────────────────────────────
	b.WriteString(styleDivider.Render(strings.Repeat("─", dividerWidth(m.width))))
	b.WriteString("\n")
	b.WriteString(m.countsBar())

	if m.err != nil {
		b.WriteString("\n")
		b.WriteString(styleError.Render("  error: " + m.err.Error()))
	}

	return b.String()
}

func (m Model) countsBar() string {
	counts := make(map[job.Status]int)
	for _, j := range m.jobs {
		counts[j.Status]++
	}
	order := []job.Status{
		job.StatusPending, job.StatusRunning, job.StatusRetrying,
		job.StatusCompleted, job.StatusFailed, job.StatusDead,
	}
	var parts []string
	for _, s := range order {
		label := fmt.Sprintf("%s:%d", string(s), counts[s])
		if st, ok := statusStyles[s]; ok {
			parts = append(parts, st.Render(label))
		} else {
			parts = append(parts, label)
		}
	}
	return "  " + strings.Join(parts, styleDim.Render("  ")) + "\n"
}

func helpView() string {
	return styleTitle.Render("Key Bindings") + "\n\n" +
		"  ↑ / k    cursor up\n" +
		"  ↓ / j    cursor down\n" +
		"  f        cycle status filter\n" +
		"  r        refresh now\n" +
		"  ?        toggle this help\n" +
		"  q        quit\n"
}

// ── Helpers ───────────────────────────────────────────────────────────────

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func priorityLabel(p job.Priority) string {
	switch p {
	case job.PriorityHigh:
		return "high"
	case job.PriorityMedium:
		return "medium"
	default:
		return "low"
	}
}

func workerLabel(id string) string {
	if id == "" {
		return "-"
	}
	return truncate(id, colWorker)
}

func age(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
}

func dividerWidth(w int) int {
	if w > 20 {
		return w
	}
	return 95
}
