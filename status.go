package block_stm

import (
	"sync"
)

type Status uint

const (
	StatusReadyToExecute Status = iota
	StatusExecuting
	StatusExecuted
	StatusAborting
	StatusSuspended
	StatusCommitted
)

// ExecutionStatus manages the execution state of a transaction
type ExecutionStatus struct {
	sync.Mutex

	incarnation Incarnation
	status      Status
	cond        *Condvar
}

// ValidationStatus manages the validation state of a transaction
type ValidationStatus struct {
	sync.Mutex

	triggeredWave uint64 // Wave counter when this transaction triggers wave validation
	requiredWave  uint64 // Current wave number when triggering specific validation
}

// StatusEntry contains both execution and validation status for a transaction
type StatusEntry struct {
	execution  ExecutionStatus
	validation ValidationStatus
}

// Execution status methods
func (s *ExecutionStatus) IsExecuted() (ok bool, incarnation Incarnation) {
	s.Lock()

	if s.status == StatusExecuted {
		ok = true
		incarnation = s.incarnation
	}

	s.Unlock()
	return
}

func (s *ExecutionStatus) TrySetExecuting() (Incarnation, bool) {
	s.Lock()

	if s.status == StatusReadyToExecute {
		s.status = StatusExecuting
		incarnation := s.incarnation

		s.Unlock()
		return incarnation, true
	}

	s.Unlock()
	return 0, false
}

func (s *ExecutionStatus) setStatus(status Status) {
	s.Lock()
	s.status = status
	s.Unlock()
}

func (s *ExecutionStatus) Resume() {
	// status must be SUSPENDED and cond != nil
	s.Lock()

	s.status = StatusExecuting
	s.cond.Notify()
	s.cond = nil

	s.Unlock()
}

func (s *ExecutionStatus) SetExecuted() {
	// status must have been EXECUTING
	s.setStatus(StatusExecuted)
}

func (s *ExecutionStatus) TryValidationAbort(incarnation Incarnation) bool {
	s.Lock()

	if s.incarnation == incarnation && s.status == StatusExecuted {
		s.status = StatusAborting

		s.Unlock()
		return true
	}

	s.Unlock()
	return false
}

func (s *ExecutionStatus) SetReadyStatus() {
	s.Lock()

	s.incarnation++
	// status must be ABORTING
	s.status = StatusReadyToExecute

	s.Unlock()
}

func (s *ExecutionStatus) Suspend(cond *Condvar) {
	s.Lock()

	s.cond = cond
	s.status = StatusSuspended

	s.Unlock()
}

func (s *ExecutionStatus) SetCommitted() {
	s.Lock()
	// status must be EXECUTED
	s.status = StatusCommitted
	s.Unlock()
}

func (s *ExecutionStatus) IsCommitted() bool {
	s.Lock()
	committed := s.status == StatusCommitted
	s.Unlock()
	return committed
}

// Validation status methods
func (s *ValidationStatus) GetTriggeredWave() uint64 {
	s.Lock()
	wave := s.triggeredWave
	s.Unlock()
	return wave
}

func (s *ValidationStatus) SetTriggeredWave(wave uint64) {
	s.Lock()
	s.triggeredWave = wave
	s.Unlock()
}

func (s *ValidationStatus) GetRequiredWave() uint64 {
	s.Lock()
	wave := s.requiredWave
	s.Unlock()
	return wave
}

func (s *ValidationStatus) SetRequiredWave(wave uint64) {
	s.Lock()
	s.requiredWave = wave
	s.Unlock()
}

// Convenience methods for StatusEntry
func (s *StatusEntry) IsExecuted() (ok bool, incarnation Incarnation) {
	return s.execution.IsExecuted()
}

func (s *StatusEntry) TrySetExecuting() (Incarnation, bool) {
	return s.execution.TrySetExecuting()
}

func (s *StatusEntry) Resume() {
	s.execution.Resume()
}

func (s *StatusEntry) SetExecuted() {
	s.execution.SetExecuted()
}

func (s *StatusEntry) TryValidationAbort(incarnation Incarnation) bool {
	return s.execution.TryValidationAbort(incarnation)
}

func (s *StatusEntry) SetReadyStatus() {
	s.execution.SetReadyStatus()
}

func (s *StatusEntry) Suspend(cond *Condvar) {
	s.execution.Suspend(cond)
}

func (s *StatusEntry) SetCommitted() {
	s.execution.SetCommitted()
}

func (s *StatusEntry) IsCommitted() bool {
	return s.execution.IsCommitted()
}

func (s *StatusEntry) GetTriggeredWave() uint64 {
	return s.validation.GetTriggeredWave()
}

func (s *StatusEntry) SetTriggeredWave(wave uint64) {
	s.validation.SetTriggeredWave(wave)
}

func (s *StatusEntry) GetRequiredWave() uint64 {
	return s.validation.GetRequiredWave()
}

func (s *StatusEntry) SetRequiredWave(wave uint64) {
	s.validation.SetRequiredWave(wave)
}