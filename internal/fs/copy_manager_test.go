package fs

import (
	"encoding/json"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/tache"
)

func copySchedulerTask(storage *copyLimiterTestDriver, ids ...string) *FileTransferTask {
	t := &FileTransferTask{
		TaskData: TaskData{
			SrcStorage:   storage,
			SrcStorageMp: storage.storage.MountPath,
		},
	}
	t.MaxRetry = 1
	if len(ids) > 0 {
		t.ID = ids[0]
	}
	return t
}

func newCopySchedulerTestManager(workers int64) *copyTaskManager {
	return &copyTaskManager{
		tasks:       make(map[string]*FileTransferTask),
		admitted:    make(map[string]copyTaskAdmission),
		activeBySrc: make(map[string]int),
		workers:     workers,
		started:     true,
		backend:     tache.NewManager[*FileTransferTask](tache.WithWorks(0)),
	}
}

func TestCopyTaskManagerSkipsFull115Queue(t *testing.T) {
	pan115 := &copyLimiterTestDriver{
		storage: model.Storage{MountPath: "/test-copy-115-full"},
		name:    "115 Cloud",
	}
	other := &copyLimiterTestDriver{
		storage: model.Storage{MountPath: "/test-copy-other"},
		name:    "189 Cloud",
	}
	m := &copyTaskManager{
		pending:     []*FileTransferTask{copySchedulerTask(pan115), copySchedulerTask(other)},
		activeBySrc: map[string]int{pan115.storage.MountPath: pan115CopyLimit},
	}

	m.mu.Lock()
	idx := m.nextRunnableLocked()
	m.mu.Unlock()
	if idx != 1 {
		t.Fatalf("selected pending index = %d, want other storage at index 1", idx)
	}
}

func TestCopyTaskManagerUses115WhenQuotaIsAvailable(t *testing.T) {
	pan115 := &copyLimiterTestDriver{
		storage: model.Storage{MountPath: "/test-copy-115-available"},
		name:    "115 Open",
	}
	other := &copyLimiterTestDriver{
		storage: model.Storage{MountPath: "/test-copy-other-available"},
		name:    "189 Cloud",
	}
	m := &copyTaskManager{
		pending:     []*FileTransferTask{copySchedulerTask(pan115), copySchedulerTask(other)},
		activeBySrc: map[string]int{pan115.storage.MountPath: pan115CopyLimit - 1},
	}

	m.mu.Lock()
	idx := m.nextRunnableLocked()
	m.mu.Unlock()
	if idx != 0 {
		t.Fatalf("selected pending index = %d, want 115 storage at index 0", idx)
	}
}

func TestCopyTaskManagerFillsRemainingWorkersWithOtherStorages(t *testing.T) {
	pan115 := &copyLimiterTestDriver{
		storage: model.Storage{MountPath: "/test-copy-115-mixed"},
		name:    "115 Cloud",
	}
	other := &copyLimiterTestDriver{
		storage: model.Storage{MountPath: "/test-copy-other-mixed"},
		name:    "189 Cloud",
	}
	m := newCopySchedulerTestManager(32)

	for i := 0; i < pan115CopyLimit; i++ {
		id := "active-115-" + string(rune('a'+i))
		t := copySchedulerTask(pan115, id)
		m.tasks[id] = t
		m.admitted[id] = copyTaskAdmission{sourceKey: pan115.storage.MountPath, is115: true}
		m.activeBySrc[pan115.storage.MountPath]++
		m.activeTotal++
	}
	for i := 0; i < 22; i++ {
		id := "pending-115-" + string(rune('a'+i))
		m.pending = append(m.pending, copySchedulerTask(pan115, id))
	}
	for i := 0; i < 22; i++ {
		id := "pending-other-" + string(rune('a'+i))
		m.pending = append(m.pending, copySchedulerTask(other, id))
	}

	m.schedule()

	if m.activeTotal != 32 {
		t.Fatalf("active task count = %d, want 32", m.activeTotal)
	}
	if got := m.activeBySrc[pan115.storage.MountPath]; got != pan115CopyLimit {
		t.Fatalf("active 115 task count = %d, want %d", got, pan115CopyLimit)
	}
	if len(m.pending) != 22 {
		t.Fatalf("pending task count = %d, want 22 remaining 115 tasks", len(m.pending))
	}
	for _, task := range m.pending {
		if task.SrcStorage != pan115 {
			t.Fatalf("pending task %q is not a 115 task", task.GetID())
		}
	}
}

func TestCopyTaskManagerReleasesSlotAndSchedulesNext(t *testing.T) {
	pan115 := &copyLimiterTestDriver{
		storage: model.Storage{MountPath: "/test-copy-115-release"},
		name:    "115 Cloud",
	}
	other := &copyLimiterTestDriver{
		storage: model.Storage{MountPath: "/test-copy-other-release"},
		name:    "189 Cloud",
	}
	active := copySchedulerTask(pan115, "active")
	next := copySchedulerTask(pan115, "next")
	m := newCopySchedulerTestManager(1)
	m.tasks[active.GetID()] = active
	m.tasks[next.GetID()] = next
	m.admitted[active.GetID()] = copyTaskAdmission{sourceKey: pan115.storage.MountPath, is115: true}
	m.activeBySrc[pan115.storage.MountPath] = 1
	m.activeTotal = 1
	m.pending = []*FileTransferTask{next}

	m.taskFinished(active)

	if m.activeTotal != 1 {
		t.Fatalf("active task count after replacement = %d, want 1", m.activeTotal)
	}
	if m.activeBySrc[pan115.storage.MountPath] != 1 {
		t.Fatalf("active 115 task count after replacement = %d, want 1", m.activeBySrc[pan115.storage.MountPath])
	}
	if _, ok := m.admitted[next.GetID()]; !ok {
		t.Fatal("next 115 task was not admitted")
	}

	// A non-115 task is selected when the 115 source has reached its quota.
	otherTask := copySchedulerTask(other, "other")
	m.pending = []*FileTransferTask{otherTask}
	m.activeTotal = 1
	m.workers = 2
	m.schedule()
	if _, ok := m.admitted[otherTask.GetID()]; !ok {
		t.Fatal("other storage task was not admitted into the free global slot")
	}
}

func TestCopyTaskManagerCancelPendingTaskRemovesItFromQueue(t *testing.T) {
	other := &copyLimiterTestDriver{
		storage: model.Storage{MountPath: "/test-copy-cancel"},
		name:    "189 Cloud",
	}
	m := NewCopyTaskManager(0, 1, nil, nil)
	task := copySchedulerTask(other, "cancel-pending")
	m.Add(task)

	m.Cancel(task.GetID())

	if task.GetState() != tache.StateCanceled {
		t.Fatalf("canceled task state = %v, want %v", task.GetState(), tache.StateCanceled)
	}
	if len(m.pending) != 0 {
		t.Fatalf("pending task count after cancel = %d, want 0", len(m.pending))
	}
}

func TestCopyTaskManagerRecoversPersistedPendingTasks(t *testing.T) {
	other := &copyLimiterTestDriver{
		storage: model.Storage{MountPath: "/test-copy-recover"},
		name:    "189 Cloud",
	}
	persisted := copySchedulerTask(other, "recover-pending")
	persisted.MaxRetry = 1
	data, err := json.Marshal([]*FileTransferTask{persisted})
	if err != nil {
		t.Fatalf("marshal persisted task: %v", err)
	}

	m := NewCopyTaskManager(0, 3, func() ([]byte, error) {
		return data, nil
	}, nil)
	m.Start()

	task, ok := m.GetByID(persisted.GetID())
	if !ok {
		t.Fatal("persisted task was not recovered")
	}
	if task.GetState() != tache.StatePending {
		t.Fatalf("recovered task state = %v, want %v", task.GetState(), tache.StatePending)
	}
	if len(m.pending) != 1 {
		t.Fatalf("recovered pending task count = %d, want 1", len(m.pending))
	}
}
