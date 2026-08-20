package services

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/google/uuid"
)

// HCJobStatus represents the state of a health-check job
type HCJobStatus string

const (
	HCJobPending HCJobStatus = "pending"
	HCJobRunning HCJobStatus = "running"
	HCJobDone    HCJobStatus = "done"
	HCJobFailed  HCJobStatus = "failed"
)

// HCJob holds state for one async pool health-check run
type HCJob struct {
	ID         string      `json:"id"`
	PoolID     int         `json:"pool_id"`
	PoolName   string      `json:"pool_name"`
	Status     HCJobStatus `json:"status"`
	Progress   int         `json:"progress"` // checked so far
	Total      int         `json:"total"`    // total proxies
	Active     int         `json:"active"`
	Failed     int         `json:"failed"`
	CheckURL   string      `json:"check_url"`
	Workers    int         `json:"workers"`
	Error      string      `json:"error,omitempty"`
	StartedAt  time.Time   `json:"started_at"`
	UpdatedAt  time.Time   `json:"updated_at"`
	FinishedAt *time.Time  `json:"finished_at,omitempty"`
	// Full results (populated when done)
	Results []models.ProxyTestResult `json:"results,omitempty"`
}

// HCJobStore keeps in-memory map of recent jobs (TTL 30 min)
type HCJobStore struct {
	mu   sync.RWMutex
	jobs map[string]*HCJob
}

var globalJobStore = &HCJobStore{
	jobs: make(map[string]*HCJob),
}

// GetJobStore returns the singleton job store
func GetJobStore() *HCJobStore {
	return globalJobStore
}

// Create registers a new job and returns it
func (s *HCJobStore) Create(poolID int, poolName, checkURL string, workers, total int) *HCJob {
	now := time.Now()
	job := &HCJob{
		ID:        uuid.New().String(),
		PoolID:    poolID,
		PoolName:  poolName,
		Status:    HCJobPending,
		CheckURL:  checkURL,
		Workers:   workers,
		Total:     total,
		StartedAt: now,
		UpdatedAt: now,
	}
	s.mu.Lock()
	s.cleanupLocked(now)
	s.jobs[job.ID] = job
	s.mu.Unlock()
	return cloneHCJob(job)
}

// Get returns a job by ID
func (s *HCJobStore) Get(id string) (*HCJob, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	return cloneHCJob(j), ok
}

// Update mutates a job (caller must hold no lock)
func (s *HCJobStore) Update(id string, fn func(*HCJob)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j, ok := s.jobs[id]; ok {
		fn(j)
		j.UpdatedAt = time.Now()
	}
}

// cleanup removes only completed jobs older than 30 minutes.
func (s *HCJobStore) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupLocked(time.Now())
}

func (s *HCJobStore) cleanupLocked(now time.Time) {
	cutoff := now.Add(-30 * time.Minute)
	for id, j := range s.jobs {
		if j.Status == HCJobPending || j.Status == HCJobRunning {
			continue
		}
		lastUpdate := j.UpdatedAt
		if j.FinishedAt != nil {
			lastUpdate = *j.FinishedAt
		}
		if lastUpdate.Before(cutoff) {
			delete(s.jobs, id)
		}
	}
}

// ListByPool returns all jobs for a given pool (newest first)
func (s *HCJobStore) ListByPool(poolID int) []*HCJob {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*HCJob
	for _, j := range s.jobs {
		if j.PoolID == poolID {
			out = append(out, cloneHCJob(j))
		}
	}
	sort.Slice(out, func(i, k int) bool {
		return out[i].StartedAt.After(out[k].StartedAt)
	})
	return out
}

func cloneHCJob(job *HCJob) *HCJob {
	if job == nil {
		return nil
	}
	cloned := *job
	if job.FinishedAt != nil {
		finishedAt := *job.FinishedAt
		cloned.FinishedAt = &finishedAt
	}
	cloned.Results = make([]models.ProxyTestResult, len(job.Results))
	for i := range job.Results {
		cloned.Results[i] = job.Results[i]
		if job.Results[i].ResponseTime != nil {
			responseTime := *job.Results[i].ResponseTime
			cloned.Results[i].ResponseTime = &responseTime
		}
		if job.Results[i].Error != nil {
			message := *job.Results[i].Error
			cloned.Results[i].Error = &message
		}
		if job.Results[i].FailureKind != nil {
			failureKind := *job.Results[i].FailureKind
			cloned.Results[i].FailureKind = &failureKind
		}
	}
	return &cloned
}

// RunPoolHealthCheckAsync starts the health check in a goroutine and returns job_id immediately.
// It calls poolSvc.HealthCheckPoolWithProgress which updates the job store as proxies are checked.
func RunPoolHealthCheckAsync(
	ctx context.Context,
	poolSvc *PoolService,
	poolID int,
	poolName, checkURL string,
	workers int,
) (*HCJob, error) {
	store := GetJobStore()
	if poolName == "" {
		poolName = fmt.Sprintf("Pool #%d", poolID)
	}
	if workers <= 0 {
		workers = 20
	}

	// Get proxy count upfront so frontend can show progress %
	proxies, _ := poolSvc.poolRepo.GetProxies(ctx, poolID)

	job := store.Create(poolID, poolName, checkURL, workers, len(proxies))

	if !poolSvc.goTask(func(taskCtx context.Context) {
		store.Update(job.ID, func(j *HCJob) {
			j.Status = HCJobRunning
		})

		jobCtx, cancel := context.WithTimeout(taskCtx, poolHealthTaskTimeout)
		defer cancel()
		result, err := poolSvc.HealthCheckPoolWithProgress(
			jobCtx,
			poolID, checkURL, workers,
			func(checked, active, failed int) {
				store.Update(job.ID, func(j *HCJob) {
					j.Progress = checked
					j.Active = active
					j.Failed = failed
				})
			},
		)
		if err == nil && jobCtx.Err() != nil {
			err = jobCtx.Err()
		}

		now := time.Now()
		if err != nil {
			store.Update(job.ID, func(j *HCJob) {
				j.Status = HCJobFailed
				j.Error = err.Error()
				j.FinishedAt = &now
			})
			return
		}

		store.Update(job.ID, func(j *HCJob) {
			j.Status = HCJobDone
			j.Total = result.Checked
			j.Active = result.Active
			j.Failed = result.Failed
			j.Progress = result.Checked
			j.Results = result.Results
			j.FinishedAt = &now
		})
	}) {
		now := time.Now()
		store.Update(job.ID, func(j *HCJob) {
			j.Status = HCJobFailed
			j.Error = "health check service is shutting down"
			j.FinishedAt = &now
		})
		return nil, fmt.Errorf("health check service is shutting down")
	}

	return job, nil
}
