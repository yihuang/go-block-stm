package block_stm

import (
	"testing"
)

func TestRollingCommitBasic(t *testing.T) {
	blockSize := 5
	scheduler := NewScheduler(blockSize)

	// Test initial state
	if scheduler.commit_idx.Load() != 0 {
		t.Errorf("Expected commit_idx to be 0, got %d", scheduler.commit_idx.Load())
	}

	// Simulate execution of transaction 0
	scheduler.txn_status[0].SetExecuted()

	// Schedule validation for transaction 0 only (specific validation)
	scheduler.required_wave[0].Store(scheduler.GetValidationWave())

	// Simulate successful validation of transaction 0
	if ok, incarnation := scheduler.txn_status[0].IsExecuted(); ok {
		if scheduler.TryCommit(0, incarnation) {
			scheduler.txn_status[0].SetCommitted()
		}
	}

	// Transaction 0 should be committed
	if !scheduler.IsCommitted(0) {
		t.Error("Transaction 0 should be committed")
	}
	if scheduler.commit_idx.Load() != 1 {
		t.Errorf("Expected commit_idx to be 1, got %d", scheduler.commit_idx.Load())
	}
}

func TestRollingCommitWaveValidation(t *testing.T) {
	blockSize := 3
	scheduler := NewScheduler(blockSize)

	// Execute transactions 0, 1, 2
	for i := 0; i < blockSize; i++ {
		scheduler.txn_status[i].SetExecuted()
	}

	// Trigger wave validation from transaction 0
	scheduler.DecreaseValidationIdx(0)

	// Check that triggered_wave is set for transactions 0 (and maybe 1, 2 depending on implementation)
	wave := scheduler.GetValidationWave()
	// According to our implementation, DecreaseValidationIdx sets triggered_wave for i <= target
	if scheduler.triggered_wave[0].Load() != wave {
		t.Errorf("Transaction 0 should have triggered_wave = %d, got %d", wave, scheduler.triggered_wave[0].Load())
	}

	// commit_wave[0] should be initialized to 0
	if scheduler.commit_wave[0].Load() != 0 {
		t.Errorf("commit_wave[0] should be 0, got %d", scheduler.commit_wave[0].Load())
	}
}

func TestRollingCommitOrder(t *testing.T) {
	blockSize := 3
	scheduler := NewScheduler(blockSize)

	// Execute all transactions
	for i := 0; i < blockSize; i++ {
		scheduler.txn_status[i].SetExecuted()
	}

	// Try to commit transaction 1 before transaction 0 (should fail)
	if ok, incarnation := scheduler.txn_status[1].IsExecuted(); ok {
		if scheduler.TryCommit(1, incarnation) {
			t.Error("Transaction 1 should not commit before transaction 0")
		}
	}

	// Commit transaction 0
	scheduler.required_wave[0].Store(scheduler.GetValidationWave())
	if ok, incarnation := scheduler.txn_status[0].IsExecuted(); ok {
		if scheduler.TryCommit(0, incarnation) {
			scheduler.txn_status[0].SetCommitted()
		}
	}

	if !scheduler.IsCommitted(0) {
		t.Error("Transaction 0 should be committed")
	}
	if scheduler.commit_idx.Load() != 1 {
		t.Errorf("Expected commit_idx to be 1, got %d", scheduler.commit_idx.Load())
	}

	// Now transaction 1 should be able to commit
	scheduler.required_wave[1].Store(scheduler.GetValidationWave())
	if ok, incarnation := scheduler.txn_status[1].IsExecuted(); ok {
		if scheduler.TryCommit(1, incarnation) {
			scheduler.txn_status[1].SetCommitted()
		}
	}

	if !scheduler.IsCommitted(1) {
		t.Error("Transaction 1 should be committed after transaction 0")
	}
}

func TestRollingCommitSkipCommitted(t *testing.T) {
	blockSize := 3
	scheduler := NewScheduler(blockSize)

	// Execute and commit transaction 0
	scheduler.txn_status[0].SetExecuted()
	scheduler.required_wave[0].Store(scheduler.GetValidationWave())
	if ok, incarnation := scheduler.txn_status[0].IsExecuted(); ok {
		if scheduler.TryCommit(0, incarnation) {
			scheduler.txn_status[0].SetCommitted()
		}
	}

	// Execute transaction 1
	scheduler.txn_status[1].SetExecuted()

	// Test that committed transaction is not returned by IsExecuted
	// (which is used by NextVersionToValidate)
	if ok, _ := scheduler.txn_status[0].IsExecuted(); ok {
		t.Error("Committed transaction 0 should not return true from IsExecuted")
	}

	// Transaction 1 should still be executable
	if ok, _ := scheduler.txn_status[1].IsExecuted(); !ok {
		t.Error("Transaction 1 should return true from IsExecuted")
	}
}