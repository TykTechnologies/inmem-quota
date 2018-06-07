package inmem_quota

import (
	"testing"
	"time"
)

func TestInMemQuotaSingle(t *testing.T) {
	qs := NewQuotasList()
	k1 := "1"
	k2 := "2"

	qs.NewQuota(k1, 10, 20, 0)
	qs.NewQuota(k2, 10, 15, 0)

	for i := 0; i <= 50; i++ {
		if err := qs.Incr(k1); err != nil {
			t.Error(err)
		}
		if err := qs.Incr(k2); err != nil {
			t.Error(err)
		}

		if qs.IsBlocked(k1) && i < 20 {
			t.Fatal("quota k1 is less than max and is blocked")
		}

		if qs.IsBlocked(k2) && i < 15 {
			t.Fatal("quota k2 is less than max and is blocked")
		}

		// Allow a margin since everything is async
		if i > 15 && !qs.IsBlocked(k2) {
			t.Errorf("k2 should fail after ~20 increments (i is: %v and k2 count is: %v)", i, qs.Get(k2))
		}
		if i > 220 && !qs.IsBlocked(k1) {
			t.Errorf("k1 should fail after ~25 increments (i is: %v and k1 count is: %v)", i, qs.Get(k2))
		}

		// Need this to simulate reality
		time.Sleep(1 * time.Millisecond)
	}
}

func TestWithMerge(t *testing.T) {
	qs := NewQuotasList()
	k1 := "1"

	qs.NewQuota(k1, 10, 180, 0)

	preppedQuota := map[string]QuotaCtr{
		k1: {
			Total:     100,
			Allowance: 180,
			period:    10,
		},
	}

	for i := 0; i <= 300; i++ {
		if err := qs.Incr(k1); err != nil {
			t.Error(err)
		}

		if i == 50 {
			// merge external snapshot
			qs.Merge(preppedQuota)
			// Total should now be 100
		}

		if i > (50+80)+5 && !qs.IsBlocked(k1) {
			// after 50, total should be 100, so block after 80 more requests
			t.Errorf("k1 should fail after ~130 increments (i is: %v and k1 count is: %v)", i, qs.Get(k1))
		}

		// Need this to simulate reality
		time.Sleep(1 * time.Millisecond)
	}
}

func TestAge(t *testing.T) {
	q := NewQuotaCtr(10, 100, 0)
	now := time.Now()
	oneMonthAgo := time.Unix(now.Unix()-(60*60*24*30), 0)
	oneDayAgo := time.Unix(now.Unix()-(60*60*24), 0)

	q.lastUpdateAt = oneMonthAgo

	if !q.ReadyToDelete(((60 * 60) * 24) * 20) {
		t.Error("quota should be ready to delete")
	}

	q.lastUpdateAt = oneDayAgo
	if !q.ReadyToDelete(((60 * 60) * 24) * 2) {
		t.Error("quota should be ready to delete")
	}

	q.lastUpdateAt = time.Now()
	if !q.ReadyToDelete(((60 * 60) * 24) * 2) {
		t.Error("quota should not be ready to delete!")
	}
}
