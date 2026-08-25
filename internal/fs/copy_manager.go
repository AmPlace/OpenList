package fs

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/task"
	"github.com/OpenListTeam/tache"
	gonanoid "github.com/matoous/go-nanoid/v2"
)

const copyTaskPersistDebounce = 3 * time.Second

type copyTaskAdmission struct {
	sourceKey string
	is115     bool
}

type copyTaskManager struct {
	mu          sync.Mutex
	tasks       map[string]*FileTransferTask
	pending     []*FileTransferTask
	admitted    map[string]copyTaskAdmission
	activeBySrc map[string]int
	activeTotal int
	workers     int64
	maxRetry    int
	started     bool

	backend      *tache.Manager[*FileTransferTask]
	read         func() ([]byte, error)
	write        func([]byte) error
	persistMu    sync.Mutex
	persistTimer *time.Timer
}

// NewCopyTaskManager creates the copy scheduler and its execution backend.
func NewCopyTaskManager(workers, maxRetry int, read func() ([]byte, error), write func([]byte) error) *copyTaskManager {
	m := &copyTaskManager{
		tasks:       make(map[string]*FileTransferTask),
		admitted:    make(map[string]copyTaskAdmission),
		activeBySrc: make(map[string]int),
		workers:     int64(workers),
		maxRetry:    maxRetry,
		read:        read,
		write:       write,
	}
	// The backend only executes admitted tasks. Its persistence callback is
	// redirected to the scheduler so pending tasks are persisted as well.
	m.backend = tache.NewManager[*FileTransferTask](
		tache.WithWorks(workers),
		tache.WithMaxRetry(maxRetry),
		tache.WithRunning(false),
		tache.WithPersistFunction(
			func() ([]byte, error) { return []byte("[]"), nil },
			func([]byte) error {
				m.persist()
				return nil
			},
		),
	)
	return m
}

// Start begins execution after the manager has been assigned to the package
// global used by FileTransferTask callbacks.
func (m *copyTaskManager) Start() {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return
	}
	m.started = true
	m.mu.Unlock()

	m.backend.Start()
	m.recover()
	m.schedule()
}

func (m *copyTaskManager) recover() {
	if m.read == nil {
		return
	}
	data, err := m.read()
	if err != nil || len(data) == 0 {
		return
	}
	var tasks []*FileTransferTask
	if err = json.Unmarshal(data, &tasks); err != nil {
		return
	}
	for _, t := range tasks {
		m.add(t)
	}
}

func (m *copyTaskManager) Add(t *FileTransferTask) {
	m.add(t)
}

func (m *copyTaskManager) add(t *FileTransferTask) {
	ctx, cancel := context.WithCancel(context.Background())
	t.SetCtx(ctx)
	t.SetCancelFunc(cancel)
	t.SetPersist(m.persist)
	if t.GetID() == "" {
		t.SetID(gonanoid.Must())
	}
	if _, maxRetry := t.GetRetry(); maxRetry == 0 {
		t.SetRetry(0, m.maxRetry)
	}
	switch t.GetState() {
	case tache.StateRunning:
		t.SetState(tache.StatePending)
	case tache.StateFailing:
		t.SetState(tache.StateFailed)
	case tache.StateCanceling:
		t.SetState(tache.StateCanceled)
		t.SetErr(context.Canceled)
	}

	m.mu.Lock()
	m.tasks[t.GetID()] = t
	if !isTaskFinished(t.GetState()) {
		m.pending = append(m.pending, t)
	}
	m.mu.Unlock()
	m.persist()
	m.schedule()
}

func isTaskFinished(state tache.State) bool {
	switch state {
	case tache.StateSucceeded, tache.StateCanceled, tache.StateErrored, tache.StateFailed:
		return true
	default:
		return false
	}
}

func (m *copyTaskManager) schedule() {
	var admitted []*FileTransferTask
	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return
	}
	for m.activeTotal < int(m.workers) {
		idx := m.nextRunnableLocked()
		if idx < 0 {
			break
		}
		t := m.pending[idx]
		m.pending = append(m.pending[:idx], m.pending[idx+1:]...)
		admission := m.admission(t)
		m.admitted[t.GetID()] = admission
		m.activeTotal++
		if admission.is115 {
			m.activeBySrc[admission.sourceKey]++
		}
		admitted = append(admitted, t)
	}
	m.mu.Unlock()

	for _, t := range admitted {
		m.backend.Add(t)
	}
}

func (m *copyTaskManager) nextRunnableLocked() int {
	for i, t := range m.pending {
		admission := m.admission(t)
		if admission.is115 {
			limiter := m.limiter(t)
			if limiter != nil && m.activeBySrc[admission.sourceKey] >= limiter.currentLimitForScheduling() {
				continue
			}
		}
		return i
	}
	return -1
}

func (m *copyTaskManager) admission(t *FileTransferTask) copyTaskAdmission {
	storage := copyTaskSource(t)
	if storage == nil {
		return copyTaskAdmission{sourceKey: t.SrcStorageMp}
	}
	return copyTaskAdmission{
		sourceKey: storage.GetStorage().MountPath,
		is115:     is115CopySource(storage),
	}
}

func (m *copyTaskManager) limiter(t *FileTransferTask) *copyStorageLimiter {
	storage := copyTaskSource(t)
	if storage == nil || !is115CopySource(storage) {
		return nil
	}
	return pan115CopyLimiters.get(storage)
}

func copyTaskSource(t *FileTransferTask) driver.Driver {
	if t.SrcStorage != nil {
		return t.SrcStorage
	}
	if t.SrcStorageMp == "" {
		return nil
	}
	storage, _ := op.GetStorageByMountPath(t.SrcStorageMp)
	return storage
}

func (m *copyTaskManager) taskFinished(t *FileTransferTask) {
	m.mu.Lock()
	admission, ok := m.admitted[t.GetID()]
	if ok {
		delete(m.admitted, t.GetID())
		m.activeTotal--
		if admission.is115 {
			m.activeBySrc[admission.sourceKey]--
		}
	}
	m.mu.Unlock()
	if ok {
		m.backend.Remove(t.GetID())
		m.persist()
		m.schedule()
	}
}

func (m *copyTaskManager) Cancel(id string) {
	t, ok := m.GetByID(id)
	if !ok {
		return
	}
	state := t.GetState()
	m.mu.Lock()
	_, admitted := m.admitted[id]
	if !admitted {
		m.removePendingLocked(id)
	}
	m.mu.Unlock()
	if isTaskFinished(state) {
		return
	}
	t.Cancel()
	if !admitted || state == tache.StatePending {
		t.SetState(tache.StateCanceled)
		t.SetErr(context.Canceled)
		m.taskFinished(t)
	}
	m.persist()
}

func (m *copyTaskManager) CancelAll() {
	for _, t := range m.GetAll() {
		m.Cancel(t.GetID())
	}
}

func (m *copyTaskManager) CancelByCondition(condition func(*FileTransferTask) bool) {
	for _, t := range m.GetByCondition(condition) {
		m.Cancel(t.GetID())
	}
}

func (m *copyTaskManager) GetAll() []*FileTransferTask {
	m.mu.Lock()
	defer m.mu.Unlock()
	tasks := make([]*FileTransferTask, 0, len(m.tasks))
	for _, t := range m.tasks {
		tasks = append(tasks, t)
	}
	return tasks
}

func (m *copyTaskManager) GetByID(id string) (*FileTransferTask, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	return t, ok
}

func (m *copyTaskManager) GetByState(states ...tache.State) []*FileTransferTask {
	return m.GetByCondition(func(t *FileTransferTask) bool {
		for _, state := range states {
			if t.GetState() == state {
				return true
			}
		}
		return false
	})
}

func (m *copyTaskManager) GetByCondition(condition func(*FileTransferTask) bool) []*FileTransferTask {
	m.mu.Lock()
	defer m.mu.Unlock()
	tasks := make([]*FileTransferTask, 0)
	for _, t := range m.tasks {
		if condition(t) {
			tasks = append(tasks, t)
		}
	}
	return tasks
}

func (m *copyTaskManager) Remove(id string) {
	m.mu.Lock()
	delete(m.tasks, id)
	m.removePendingLocked(id)
	_, admitted := m.admitted[id]
	m.mu.Unlock()
	if admitted {
		m.backend.Remove(id)
	}
	m.persist()
}

func (m *copyTaskManager) removePendingLocked(id string) {
	for i, t := range m.pending {
		if t.GetID() == id {
			m.pending = append(m.pending[:i], m.pending[i+1:]...)
			return
		}
	}
}

func (m *copyTaskManager) RemoveAll() {
	for _, t := range m.GetAll() {
		m.Remove(t.GetID())
	}
}

func (m *copyTaskManager) RemoveByState(states ...tache.State) {
	for _, t := range m.GetByState(states...) {
		m.Remove(t.GetID())
	}
}

func (m *copyTaskManager) RemoveByCondition(condition func(*FileTransferTask) bool) {
	for _, t := range m.GetByCondition(condition) {
		m.Remove(t.GetID())
	}
}

func (m *copyTaskManager) Retry(id string) {
	t, ok := m.GetByID(id)
	if !ok {
		return
	}
	m.mu.Lock()
	_, admitted := m.admitted[id]
	m.mu.Unlock()
	if admitted {
		return
	}
	t.SetState(tache.StateWaitingRetry)
	t.SetErr(nil)
	t.SetRetry(0, m.maxRetry)
	m.mu.Lock()
	m.removePendingLocked(id)
	m.pending = append(m.pending, t)
	m.mu.Unlock()
	m.persist()
	m.schedule()
}

func (m *copyTaskManager) RetryAllFailed() {
	for _, t := range m.GetByState(tache.StateFailed) {
		m.Retry(t.GetID())
	}
}

func (m *copyTaskManager) SetWorkersNumActive(active int64) {
	if active < 0 {
		active = 0
	}
	m.mu.Lock()
	m.workers = active
	m.mu.Unlock()
	m.backend.SetWorkersNumActive(active)
	m.schedule()
}

func (m *copyTaskManager) persist() {
	if m.write == nil {
		return
	}
	m.persistMu.Lock()
	if m.persistTimer == nil {
		m.persistTimer = time.AfterFunc(copyTaskPersistDebounce, func() {
			m.persistNow()
		})
	} else {
		m.persistTimer.Reset(copyTaskPersistDebounce)
	}
	m.persistMu.Unlock()
}

func (m *copyTaskManager) persistNow() {
	m.persistMu.Lock()
	m.persistTimer = nil
	m.persistMu.Unlock()

	m.mu.Lock()
	tasks := make([]*FileTransferTask, 0, len(m.tasks))
	for _, t := range m.tasks {
		tasks = append(tasks, t)
	}
	m.mu.Unlock()
	data, err := json.Marshal(tasks)
	if err == nil {
		_ = m.write(data)
	}
}

var _ task.Manager[*FileTransferTask] = (*copyTaskManager)(nil)
