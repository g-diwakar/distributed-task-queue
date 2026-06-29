package job

import (
	"fmt"
)

// JobSpec declares the expected payload shape for a job type.
type JobSpec struct {
	Required []string
	Optional []string
}

var specs = map[JobType]JobSpec{
	TypeSleepJob:      {Optional: []string{"duration_seconds"}},
	TypeFailJob:       {},
	TypeHTTPFetch:     {Required: []string{"url"}},
	TypeDataTransform: {Required: []string{"input"}},
	TypeImageResize:   {Required: []string{"width", "height"}, Optional: []string{"source"}},
	TypeSendEmail:     {Required: []string{"to"}, Optional: []string{"subject", "body"}},
}

func (t JobType) Spec() (JobSpec, bool) {
	s, ok := specs[t]
	return s, ok
}

// ValidatePayload returns an error if any required payload fields are absent.
func (t JobType) ValidatePayload(payload map[string]interface{}) error {
	spec, ok := specs[t]
	if !ok {
		return fmt.Errorf("unknown job type %q", t)
	}
	for _, field := range spec.Required {
		if _, exists := payload[field]; !exists {
			return fmt.Errorf("job type %q requires payload field %q", t, field)
		}
	}
	return nil
}
