package block_stm

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

// Helper functions for combined validation index and wave
func validationWave(val uint64) uint64 {
	return val >> 32
}

func validationIdx(val uint64) uint64 {
	return val & 0xFFFFFFFF
}

func makeValidationIdxWave(wave, idx uint64) uint64 {
	return (wave << 32) | (idx & 0xFFFFFFFF)
}

// Helper functions for combined commit index and wave
func commitWave(val uint64) uint64 {
	return val >> 32
}

func commitIdx(val uint64) uint64 {
	return val & 0xFFFFFFFF
}

func makeCommitIdxWave(wave, idx uint64) uint64 {
	return (wave << 32) | (idx & 0xFFFFFFFF)
}

type TaskKind int

const (
	TaskKindExecution TaskKind = iota
	TaskKindValidation
)

type TxDependency struct {
	sync.Mutex
	dependents []TxnIndex
}

func (t *TxDependency) Swap(new []TxnIndex) []TxnIndex {
	t.Lock()
	old := t.dependents
	t.dependents = new
	t.Unlock()
	return old
}

// Scheduler implements the scheduler for the block-stm
// ref: `Algorithm 4 The Scheduler module, variables, utility APIs and next task logic`
type Scheduler struct {
	block_size int

	// An index that tracks the next transaction to try and execute.
	execution_idx atomic.Uint64
	// Combined validation index and wave (upper 32 bits: wave, lower 32 bits: index)
	validation_idx_wave atomic.Uint64
	// Number of times validation_idx or execution_idx was decreased
	decrease_cnt atomic.Uint64
	// Number of ongoing validation and execution tasks
	num_active_tasks atomic.Uint64
	// Marker for completion
	done_marker atomic.Bool

	// txn_idx to a mutex-protected set of dependent transaction indices
	txn_dependency []TxDependency
	// txn_idx to a mutex-protected pair (incarnation_number, status), where status ∈ {READY_TO_EXECUTE, EXECUTING, EXECUTED, ABORTING}.
	txn_status []StatusEntry

	// metrics
	executedTxns  atomic.Int64
	validatedTxns atomic.Int64

	// Rolling commit counters
	commit_idx_wave atomic.Uint64           // Combined: upper 32 bits = commit_wave, lower 32 bits = commit_idx
}

func NewScheduler(block_size int) *Scheduler {
	return &Scheduler{
		block_size:     block_size,
		txn_dependency: make([]TxDependency, block_size),
		txn_status:     make([]StatusEntry, block_size),
		// commit_idx_wave is initialized to 0 (commit_idx = 0, commit_wave = 0)
	}
}

func (s *Scheduler) Done() bool {
	return s.done_marker.Load()
}

func (s *Scheduler) DecreaseValidationIdx(target TxnIndex) {
	for {
		old := s.validation_idx_wave.Load()
		oldWave := validationWave(old)
		oldIdx := validationIdx(old)

		// Only update if new index is smaller than current index
		if uint64(target) >= oldIdx {
			return
		}

		// Increment wave, set new index
		newWave := oldWave + 1
		newVal := makeValidationIdxWave(newWave, uint64(target))

		if s.validation_idx_wave.CompareAndSwap(old, newVal) {
			s.decrease_cnt.Add(1)

			// Record wave validation trigger
			limit := min(int(target)+1, s.block_size)
			for i := 0; i < limit; i++ {
				s.txn_status[i].SetTriggeredWave(newWave)
			}
			return
		}
	}
}

func (s *Scheduler) CheckDone() {
	observed_cnt := s.decrease_cnt.Load()
	if s.execution_idx.Load() >= uint64(s.block_size) &&
		validationIdx(s.validation_idx_wave.Load()) >= uint64(s.block_size) &&
		s.num_active_tasks.Load() == 0 {
		if observed_cnt == s.decrease_cnt.Load() {
			s.done_marker.Store(true)
		}
	}
	// avoid busy waiting
	runtime.Gosched()
}

// TryIncarnate tries to incarnate a transaction index to execute.
// Returns the transaction version if successful, otherwise returns invalid version.
//
// Invariant `num_active_tasks`: decreased if an invalid task is returned.
func (s *Scheduler) TryIncarnate(idx TxnIndex) TxnVersion {
	if int(idx) < s.block_size {
		if incarnation, ok := s.txn_status[idx].TrySetExecuting(); ok {
			return TxnVersion{idx, incarnation}
		}
	}
	DecrAtomic(&s.num_active_tasks)
	return InvalidTxnVersion
}

// NextVersionToExecute get the next transaction index to execute,
// returns invalid version if no task is available
//
// Invariant `num_active_tasks`: increased if a valid task is returned.
func (s *Scheduler) NextVersionToExecute() TxnVersion {
	if s.execution_idx.Load() >= uint64(s.block_size) {
		s.CheckDone()
		return InvalidTxnVersion
	}
	IncrAtomic(&s.num_active_tasks)
	idx_to_execute := s.execution_idx.Add(1) - 1
	return s.TryIncarnate(TxnIndex(idx_to_execute))
}

// NextVersionToValidate get the next transaction index to validate,
// returns invalid version if no task is available.
//
// Invariant `num_active_tasks`: increased if a valid task is returned.
func (s *Scheduler) NextVersionToValidate() TxnVersion {
	if validationIdx(s.validation_idx_wave.Load()) >= uint64(s.block_size) {
		s.CheckDone()
		return InvalidTxnVersion
	}
	IncrAtomic(&s.num_active_tasks)

	// Atomically increment just the index part while keeping wave part unchanged
	for {
		old := s.validation_idx_wave.Load()
		oldWave := validationWave(old)
		oldIdx := validationIdx(old)

		if oldIdx >= uint64(s.block_size) {
			DecrAtomic(&s.num_active_tasks)
			return InvalidTxnVersion
		}

		newIdx := oldIdx + 1
		newVal := makeValidationIdxWave(oldWave, newIdx)

		if s.validation_idx_wave.CompareAndSwap(old, newVal) {
			if oldIdx < uint64(s.block_size) {
				if ok, incarnation := s.txn_status[oldIdx].IsExecuted(); ok {
					// Check if transaction is already committed
					if !s.txn_status[oldIdx].IsCommitted() {
						return TxnVersion{TxnIndex(oldIdx), incarnation}
					}
				}
			}
			DecrAtomic(&s.num_active_tasks)
			return InvalidTxnVersion
		}
	}
}

// NextTask returns the transaction index and task kind for the next task to execute or validate,
// returns invalid version if no task is available.
//
// Invariant `num_active_tasks`: increased if a valid task is returned.
func (s *Scheduler) NextTask() (TxnVersion, TaskKind) {
	validation_idx := validationIdx(s.validation_idx_wave.Load())
	execution_idx := s.execution_idx.Load()
	if validation_idx < execution_idx {
		return s.NextVersionToValidate(), TaskKindValidation
	} else {
		return s.NextVersionToExecute(), TaskKindExecution
	}
}

func (s *Scheduler) WaitForDependency(txn TxnIndex, blocking_txn TxnIndex) *Condvar {
	cond := NewCondvar()
	entry := &s.txn_dependency[blocking_txn]
	entry.Lock()

	// thread holds 2 locks
	if ok, _ := s.txn_status[blocking_txn].IsExecuted(); ok {
		// dependency resolved before locking in Line 148
		entry.Unlock()
		return nil
	}

	s.txn_status[txn].Suspend(cond)
	entry.dependents = append(entry.dependents, txn)
	entry.Unlock()

	return cond
}

func (s *Scheduler) ResumeDependencies(txns []TxnIndex) {
	for _, txn := range txns {
		s.txn_status[txn].Resume()
	}
}

// Invariant `num_active_tasks`: decreased if an invalid task is returned.
func (s *Scheduler) FinishExecution(version TxnVersion, wroteNewPath bool) (TxnVersion, TaskKind) {
	s.txn_status[version.Index].SetExecuted()

	deps := s.txn_dependency[version.Index].Swap(nil)
	s.ResumeDependencies(deps)
	if validationIdx(s.validation_idx_wave.Load()) > uint64(version.Index) { // otherwise index already small enough
		if !wroteNewPath {
			// schedule validation for current tx only, don't decrease num_active_tasks
			// Record the current wave number for this specific validation
			s.txn_status[version.Index].SetRequiredWave(validationWave(s.validation_idx_wave.Load()))
			return version, TaskKindValidation
		}
		// schedule validation for txn_idx and higher txns
		s.DecreaseValidationIdx(version.Index)
	}
	DecrAtomic(&s.num_active_tasks)
	return InvalidTxnVersion, 0
}

func (s *Scheduler) TryValidationAbort(version TxnVersion) bool {
	return s.txn_status[version.Index].TryValidationAbort(version.Incarnation)
}

// Invariant `num_active_tasks`: decreased if an invalid task is returned.
func (s *Scheduler) FinishValidation(txn TxnIndex, aborted bool) (TxnVersion, TaskKind) {
	if aborted {
		s.txn_status[txn].SetReadyStatus()
		s.DecreaseValidationIdx(txn + 1)
		if s.execution_idx.Load() > uint64(txn) {
			return s.TryIncarnate(txn), TaskKindExecution
		}
	} else {
		// Validation succeeded, try to commit
		if ok, incarnation := s.txn_status[txn].IsExecuted(); ok {
			if s.TryCommit(txn, incarnation) {
				// Transaction committed - mark as committed status
				s.txn_status[txn].SetCommitted()
			}
		}
	}

	DecrAtomic(&s.num_active_tasks)
	return InvalidTxnVersion, 0
}

func (s *Scheduler) Stats() string {
	return fmt.Sprintf("executed: %d, validated: %d",
		s.executedTxns.Load(), s.validatedTxns.Load())
}

// TryCommit attempts to commit transaction txn with given incarnation.
// A transaction is committed iff:
// 1. All previous transactions are committed (commit_idx == txn)
// 2. The last validation is successful and late enough (current wave >= commit_wave and required_wave)
func (s *Scheduler) TryCommit(txn TxnIndex, incarnation Incarnation) bool {
	for {
		old := s.commit_idx_wave.Load()
		oldCommitWave := commitWave(old)
		oldCommitIdx := commitIdx(old)

		// Check condition 1: All previous transactions are committed
		if oldCommitIdx != uint64(txn) {
			return false
		}

		// Check condition 2: Validation is successful and late enough
		currentWave := validationWave(s.validation_idx_wave.Load())
		requiredWave := s.txn_status[txn].GetRequiredWave()

		if currentWave < oldCommitWave || currentWave < requiredWave {
			return false
		}

		// Compute new commit wave for next transaction
		newCommitWave := oldCommitWave
		if triggeredWave := s.txn_status[txn].GetTriggeredWave(); triggeredWave > newCommitWave {
			newCommitWave = triggeredWave
		}

		// Increment commit index, update commit wave
		newCommitIdx := oldCommitIdx + 1
		newVal := makeCommitIdxWave(newCommitWave, newCommitIdx)

		if s.commit_idx_wave.CompareAndSwap(old, newVal) {
			return true
		}
		// CAS failed, retry
	}
}

// IsCommitted returns true if transaction txn is committed.
// This is primarily for testing purposes.
func (s *Scheduler) IsCommitted(txn TxnIndex) bool {
	return s.txn_status[txn].IsCommitted()
}

// GetValidationWave returns the current validation wave.
// This is primarily for testing purposes.
func (s *Scheduler) GetValidationWave() uint64 {
	return validationWave(s.validation_idx_wave.Load())
}

// GetCommitIdx returns the current commit index.
// This is primarily for testing purposes.
func (s *Scheduler) GetCommitIdx() uint64 {
	return commitIdx(s.commit_idx_wave.Load())
}
