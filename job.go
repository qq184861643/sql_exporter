package sql_exporter

import (
	"fmt"

	"github.com/free/sql_exporter/config"
	"github.com/free/sql_exporter/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// Job is a collection of targets with the same collectors applied.
type Job interface {
	Targets() []Target
}

// job implements Job. It wraps the corresponding JobConfig and a set of Targets.
type job struct {
	config     *config.JobConfig
	targets    []Target
	logContext string
}

// NewJob returns a new Job with the given configuration.
func NewJob(jc *config.JobConfig, gc *config.GlobalConfig) (Job, errors.WithContext) {
	j := job{
		config:     jc,
		targets:    make([]Target, 0, 10),
		logContext: fmt.Sprintf("job=%q", jc.Name),
	}
	Maxconns := gc.MaxConns
	MaxIdleConns := gc.MaxIdleConns
	defer func() {
		gc.MaxConns = Maxconns
		gc.MaxIdleConns = MaxIdleConns
	}()

	if jc.MaxConns != -1 {
		gc.MaxConns = jc.MaxConns
	}
	if jc.MaxIdleConns != -1 {
		gc.MaxIdleConns = jc.MaxIdleConns
	}
	for _, sc := range jc.StaticConfigs {
		if sc.MaxConns != -1 {
			gc.MaxConns = sc.MaxConns
		}
		if sc.MaxIdleConns != -1 {
			gc.MaxIdleConns = sc.MaxIdleConns
		}
		for _, target := range sc.Targets {
			tname := target.Instance
			dsn := target.DSN
			constLabels := prometheus.Labels{
				"job":      jc.Name,
				"instance": tname,
			}
			for name, value := range sc.Labels {
				// Shouldn't happen as there are sanity checks in config, but check nonetheless.
				if _, found := constLabels[name]; found {
					return nil, errors.Errorf(j.logContext, "duplicate label %q", name)
				}
				constLabels[name] = value
			}
			t, err := NewTarget(j.logContext, tname, string(dsn), jc.Collectors(), constLabels, gc)
			if err != nil {
				return nil, err
			}
			j.targets = append(j.targets, t)
		}
	}
	return &j, nil
}

func (j *job) Targets() []Target {
	return j.targets
}
