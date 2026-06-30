package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"g-diwakar/distributed-task-queue/internal/job"
)

const pollInterval = 500 * time.Millisecond

// statusCycle is the ordered list of filter options; "" means show all.
var statusCycle = []job.Status{
	"",
	job.StatusPending,
	job.StatusRunning,
	job.StatusRetrying,
	job.StatusCompleted,
	job.StatusFailed,
	job.StatusDead,
	job.StatusCancelled,
}

type (
	tickMsg struct{}
	jobsMsg []*job.Job
	errMsg  struct{ err error }
)

type Model struct {
	baseURL   string
	jobs      []*job.Job
	cursor    int
	statusIdx int // index into statusCycle; 0 = show all
	err       error
	width     int
	height    int
	showHelp  bool
}

func New(baseURL string) Model {
	return Model{baseURL: baseURL}
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(tick(), doFetch(m.baseURL, statusCycle[m.statusIdx]))
}

func tick() tea.Cmd {
	return tea.Tick(pollInterval, func(_ time.Time) tea.Msg { return tickMsg{} })
}

func doFetch(baseURL string, status job.Status) tea.Cmd {
	return func() tea.Msg {
		url := baseURL + "/jobs"
		if status != "" {
			url += fmt.Sprintf("?status=%s", status)
		}
		resp, err := http.Get(url) //nolint:gosec
		if err != nil {
			return errMsg{err: err}
		}
		defer resp.Body.Close()

		var jobs []*job.Job
		if err := json.NewDecoder(resp.Body).Decode(&jobs); err != nil {
			return errMsg{err: err}
		}
		if jobs == nil {
			jobs = []*job.Job{}
		}

		sort.Slice(jobs, func(i, j int) bool {
			return jobs[i].CreatedAt.After(jobs[j].CreatedAt)
		})
		return jobsMsg(jobs)
	}
}
