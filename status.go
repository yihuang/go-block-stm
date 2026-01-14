package block_stm

import "sync"

type Status uint

const (
	StatusReadyToExecute Status = iota
	StatusExecuting
	StatusExecuted
	StatusAborting
	StatusSuspended
	StatusCommitted
)

// StatusEntry is a state machine for the status of a transaction, all the transitions are atomic protected by a mutex.
//
// ```mermaid
// stateDiagram-v2
//
//	[*] --> ReadyToExecute
//	ReadyToExecute --> Executing: TrySetExecuting()
//	Executing --> Executed: SetExecuted()
//	Executing --> Suspended: Suspend(cond)\nset cond
//	Executed --> Aborting: TryValidationAbort(incarnation)
//	Executed --> Committed: SetCommitted()
//	Aborting --> ReadyToExecute: SetReadyStatus()\nincarnation++
//	Suspended --> Executing: Resume()
//
// ```
type StatusEntry struct {
	sync.Mutex

	incarnation Incarnation
	status      Status

	cond *Condvar
}

func (s *StatusEntry) IsExecuted() (ok bool, incarnation Incarnation) {
	s.Lock()

	if s.status == StatusExecuted {
		ok = true
		incarnation = s.incarnation
	}

	s.Unlock()
	return
}

func (s *StatusEntry) TrySetExecuting() (Incarnation, bool) {
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

func (s *StatusEntry) setStatus(status Status) {
	s.Lock()
	s.status = status
	s.Unlock()
}

func (s *StatusEntry) Resume() {
	// status must be SUSPENDED and cond != nil
	s.Lock()

	s.status = StatusExecuting
	s.cond.Notify()
	s.cond = nil

	s.Unlock()
}

func (s *StatusEntry) SetExecuted() {
	// status must have been EXECUTING
	s.setStatus(StatusExecuted)
}

func (s *StatusEntry) TryValidationAbort(incarnation Incarnation) bool {
	s.Lock()

	if s.incarnation == incarnation && s.status == StatusExecuted {
		s.status = StatusAborting

		s.Unlock()
		return true
	}

	s.Unlock()
	return false
}

func (s *StatusEntry) SetReadyStatus() {
	s.Lock()

	s.incarnation++
	// status must be ABORTING
	s.status = StatusReadyToExecute

	s.Unlock()
}

func (s *StatusEntry) Suspend(cond *Condvar) {
	s.Lock()

	s.cond = cond
	s.status = StatusSuspended

	s.Unlock()
}

// SetCommitted marks the transaction as committed.
// Status must be EXECUTED.
func (s *StatusEntry) SetCommitted() {
	s.Lock()
	// status must be EXECUTED
	s.status = StatusCommitted
	s.Unlock()
}

// IsCommitted returns true if the transaction is committed.
func (s *StatusEntry) IsCommitted() bool {
	s.Lock()
	committed := s.status == StatusCommitted
	s.Unlock()
	return committed
}
