package main

import (
	"log"

	tea "github.com/charmbracelet/bubbletea"

	"g-diwakar/distributed-task-queue/config"
	"g-diwakar/distributed-task-queue/dashboard"
)

func main() {
	cfg := config.Load()

	p := tea.NewProgram(
		dashboard.New(cfg.Dashboard.APIURL),
		tea.WithAltScreen(),
	)

	if _, err := p.Run(); err != nil {
		log.Fatalf("dashboard error: %v", err)
	}
}
