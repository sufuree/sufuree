package demand

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type mem struct {
	mu     sync.Mutex
	claims map[string]Claim
	fail   error
}

func newMem() *mem { return &mem{claims: map[string]Claim{}} }

func (m *mem) Update(_ context.Context, fn func(Tx) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return m.fail
	}
	return fn(m)
}

func (m *mem) View(_ context.Context, fn func(Tx) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return fn(m)
}

func (m *mem) PutClaim(c Claim) error {
	m.claims[c.ClaimID] = c
	return nil
}

func (m *mem) GetClaim(id string) (Claim, error) {
	c, ok := m.claims[id]
	if !ok {
		return Claim{}, ErrNotFound
	}
	return c, nil
}

func (m *mem) ListClaims() ([]Claim, error) {
	out := make([]Claim, 0, len(m.claims))
	for _, c := range m.claims {
		out = append(out, c)
	}
	return out, nil
}

func (m *mem) DeleteClaim(id string) error {
	if _, ok := m.claims[id]; !ok {
		return ErrNotFound
	}
	delete(m.claims, id)
	return nil
}

func TestConcurrentJoinCoalescesOneClaim(t *testing.T) {
	t.Parallel()
	st := newMem()
	mgr := New(st)
	const n = 32
	ids := make(chan string, n)
	var wg sync.WaitGroup
	deadline := time.Now().Add(time.Minute)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, _, err := mgr.Join(context.Background(), "app-1", "req-"+string(rune('a'+i%26)), deadline)
			if err != nil {
				t.Errorf("join: %v", err)
				return
			}
			ids <- c.ClaimID
		}(i)
	}
	wg.Wait()
	close(ids)
	var first string
	for id := range ids {
		if first == "" {
			first = id
		} else if id != first {
			t.Fatalf("claim ids diverged: %s vs %s", first, id)
		}
	}
	claims, err := st.ListClaims()
	if err != nil {
		t.Fatal(err)
	}
	pending := 0
	for _, c := range claims {
		if c.ResourceUID == "app-1" && c.State == StatePending {
			pending++
		}
	}
	if pending != 1 {
		t.Fatalf("pending claims = %d", pending)
	}
}

func TestDistinctResourcesDoNotCoalesce(t *testing.T) {
	t.Parallel()
	mgr := New(newMem())
	deadline := time.Now().Add(time.Minute)
	a, _, err := mgr.Join(context.Background(), "a", "r1", deadline)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := mgr.Join(context.Background(), "b", "r2", deadline)
	if err != nil {
		t.Fatal(err)
	}
	if a.ClaimID == b.ClaimID {
		t.Fatal("distinct resources shared a claim")
	}
}

func TestCancelAndHandoff(t *testing.T) {
	t.Parallel()
	mgr := New(newMem())
	ctx := context.Background()
	deadline := time.Now().Add(time.Minute)
	c, ch, err := mgr.Join(ctx, "app", "req", deadline)
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.Cancel(ctx, c.ClaimID); err != nil {
		t.Fatal(err)
	}
	res := <-ch
	if !errors.Is(res.Err, ErrCancelled) {
		t.Fatalf("result = %+v", res)
	}
	if err := mgr.AcknowledgeHandoff(ctx, c.ClaimID, 3); !errors.Is(err, ErrConflict) {
		t.Fatalf("ack after cancel = %v", err)
	}

	c, ch, err = mgr.Join(ctx, "app", "req2", deadline)
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.AcknowledgeHandoff(ctx, c.ClaimID, 0); err == nil {
		t.Fatal("zero generation was accepted")
	}
	if err := mgr.AcknowledgeHandoff(ctx, c.ClaimID, 4); err != nil {
		t.Fatal(err)
	}
	res = <-ch
	if res.Err != nil || res.PlacementGeneration != 4 {
		t.Fatalf("handoff result = %+v", res)
	}
	if err := mgr.AcknowledgeHandoff(ctx, c.ClaimID, 4); err != nil {
		t.Fatal(err)
	}
	if err := mgr.AcknowledgeHandoff(ctx, c.ClaimID, 5); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting handoff = %v", err)
	}
	_, _, err = mgr.Join(ctx, "app", "req3", deadline)
	if err != nil {
		t.Fatal(err)
	}
}

func TestJoinPersistsBeforeWaiterAndSurvivesNewManager(t *testing.T) {
	t.Parallel()
	st := newMem()
	deadline := time.Now().Add(time.Minute)
	c, _, err := New(st).Join(context.Background(), "app", "req", deadline)
	if err != nil {
		t.Fatal(err)
	}
	mgr := New(st)
	again, ch, err := mgr.Join(context.Background(), "app", "other", deadline)
	if err != nil {
		t.Fatal(err)
	}
	if again.ClaimID != c.ClaimID {
		t.Fatalf("reloaded claim = %s, want %s", again.ClaimID, c.ClaimID)
	}
	if err := mgr.AcknowledgeHandoff(context.Background(), c.ClaimID, 9); err != nil {
		t.Fatal(err)
	}
	res := <-ch
	if res.PlacementGeneration != 9 || res.Err != nil {
		t.Fatalf("result = %+v", res)
	}
}

func TestStoreFailureDoesNotRecordClaim(t *testing.T) {
	t.Parallel()
	st := newMem()
	st.fail = errors.New("disk full")
	_, _, err := New(st).Join(context.Background(), "app", "req", time.Now().Add(time.Minute))
	if err == nil {
		t.Fatal("expected store error")
	}
	if len(st.claims) != 0 {
		t.Fatalf("claims recorded on failed update: %+v", st.claims)
	}
}
