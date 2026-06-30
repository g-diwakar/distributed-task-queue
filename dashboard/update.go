package dashboard

import (
	tea "github.com/charmbracelet/bubbletea"
)

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height

	case tickMsg:
		return m, tea.Batch(tick(), doFetch(m.baseURL, statusCycle[m.statusIdx]))

	case jobsMsg:
		m.jobs = msg
		m.err = nil
		if m.cursor >= len(m.jobs) {
			m.cursor = max(0, len(m.jobs)-1)
		}

	case errMsg:
		m.err = msg.err

	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if m.cursor < len(m.jobs)-1 {
				m.cursor++
			}
		case "f":
			m.statusIdx = (m.statusIdx + 1) % len(statusCycle)
			m.cursor = 0
			return m, doFetch(m.baseURL, statusCycle[m.statusIdx])
		case "r":
			return m, doFetch(m.baseURL, statusCycle[m.statusIdx])
		case "?":
			m.showHelp = !m.showHelp
		}
	}

	return m, nil
}
