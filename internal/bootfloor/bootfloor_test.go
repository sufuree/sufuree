package bootfloor

import (
	"errors"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/resource"
)

func rec(gen, inc, epoch, seq uint64) *resource.BootFloor {
	return &resource.BootFloor{Installation: "inst-1", Generation: gen, Incarnation: inc, ProgressEpoch: epoch, ProgressSeq: seq}
}

func entry(inc, gen, epoch, seq uint64) Entry {
	return Entry{Installation: "inst-1", Incarnation: inc, Generation: gen, Epoch: epoch, Seq: seq}
}

func TestMerge(t *testing.T) {
	cases := []struct {
		name string
		rec  *resource.BootFloor
		e    Entry
		want resource.BootFloor
		o    Outcome
	}{
		{"no record: seeded from the host", nil, entry(0, 50, 40, 7), *rec(50, 1, 40, 7), Created},
		{"no record: a claimed later incarnation seeds H only", nil, Entry{Installation: "inst-1", Incarnation: 5, IncarnationFrom: 9, Generation: 50}, *rec(50, 1, 0, 0), Ahead},
		{"same incarnation: maxima", rec(10, 1, 9, 3), entry(1, 12, 9, 2), *rec(12, 1, 9, 3), Raised},
		{"same incarnation: higher floor", rec(10, 1, 9, 3), entry(1, 9, 9, 4), *rec(10, 1, 9, 4), Raised},
		{"same incarnation: nothing new", rec(10, 1, 9, 3), entry(1, 10, 9, 3), *rec(10, 1, 9, 3), Unchanged},
		{"host 0 is incarnation 1", rec(10, 1, 9, 3), entry(0, 11, 9, 3), *rec(11, 1, 9, 3), Raised},
		{"older incarnation: floor dropped, H still taken", rec(10, 2, 5, 1), entry(1, 90, 80, 9), *rec(90, 2, 5, 1), Raised},
		{"older incarnation, nothing new", rec(10, 2, 5, 1), entry(1, 9, 8, 9), *rec(10, 2, 5, 1), Unchanged},
		{"newer incarnation never lowers the floor", rec(30, 1, 29, 9), entry(2, 20, 0, 0), *rec(30, 1, 29, 9), Ahead},
		{"newer incarnation: only H is taken", rec(30, 1, 29, 9), entry(2, 40, 0, 0), *rec(40, 1, 29, 9), Ahead},
		{"a generation past the bound is ignored", rec(30, 1, 29, 9), entry(1, MaxGeneration+1, MaxGeneration, 1), *rec(30, 1, 29, 9), Unchanged},
		{"malformed progress above H is dropped", nil, entry(1, 5, 9, 9), *rec(5, 1, 0, 0), Created},
	}
	for _, tc := range cases {
		got, o := Merge(tc.rec, tc.e)
		if got != tc.want || o != tc.o {
			t.Errorf("%s: Merge = %+v, %v; want %+v, %v", tc.name, got, o, tc.want, tc.o)
		}
	}
}

func TestPlanAllocate(t *testing.T) {
	c := Command{Kind: KindAllocate, Installation: "inst-1"}
	out, o, err := PlanAllocate(nil, c)
	if err != nil || out.Generation != 1 || out.Incarnation != 1 || o != Created {
		t.Fatalf("first allocation = %+v, %v, %v", out, o, err)
	}
	c.Expect = 4
	if _, _, err := PlanAllocate(rec(5, 1, 0, 0), c); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale expect: %v, want ErrConflict", err)
	}
	c.Expect = 5
	c.Local = &Entry{Incarnation: 1, Generation: 50, Epoch: 40, Seq: 7}
	out, o, err = PlanAllocate(rec(5, 1, 3, 1), c)
	if err != nil || out.Generation != 51 || out.ProgressEpoch != 40 || out.ProgressSeq != 7 || o != Raised {
		t.Fatalf("allocation above the host's record = %+v, %v, %v", out, o, err)
	}
	c.Local = &Entry{Incarnation: 2, Generation: 6}
	if _, _, err := PlanAllocate(rec(5, 1, 3, 1), c); !errors.Is(err, ErrBehindHost) {
		t.Fatalf("a host with a newer incarnation: %v, want ErrBehindHost", err)
	}
}

func lowering(by, approver string) Lowering {
	return Lowering{ExpectIncarnation: 1, Epoch: 6, Seq: 2, RequestedBy: by, ApprovedBy: approver,
		Reason: "restore after bad migration", Reference: "INC-42", At: time.Unix(100, 0).UTC()}
}

func TestPlanLower(t *testing.T) {
	r := *rec(20, 1, 19, 8)
	out, err := PlanLower(r, lowering("owner@example.com", "operator@zati.run"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Incarnation != 2 || out.IncarnationFrom != 21 || out.ProgressEpoch != 6 || out.ProgressSeq != 2 || out.Generation != 20 {
		t.Fatalf("lowered = %+v", out)
	}
	if l := out.Lowered; l == nil || l.FromIncarnation != 1 || l.FromEpoch != 19 || l.FromSeq != 8 {
		t.Fatalf("audit = %+v", out.Lowered)
	}
	if _, err := PlanLower(r, lowering("owner@example.com", "Owner@example.com")); err == nil {
		t.Fatal("one person both requested and approved a lowering")
	}
	l := lowering("a", "b")
	l.Reference = " "
	if _, err := PlanLower(r, l); err == nil {
		t.Fatal("a lowering without a reference was accepted")
	}
	l = lowering("a", "b")
	l.ExpectIncarnation = 2
	if _, err := PlanLower(r, l); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale incarnation: %v, want ErrConflict", err)
	}
	l = lowering("a", "b")
	l.TargetIncarnation = 1
	if _, err := PlanLower(r, l); err == nil {
		t.Fatal("a lowering to the current incarnation was accepted")
	}
	l.TargetIncarnation = 3 // re-applying a lowering a restored controller missed
	if out, err := PlanLower(r, l); err != nil || out.Incarnation != 3 {
		t.Fatalf("lowering to a target incarnation = %+v, %v", out, err)
	}
	l = lowering("a", "b")
	l.Epoch = 21
	if _, err := PlanLower(r, l); err == nil {
		t.Fatal("a mark naming a generation never issued was accepted")
	}
}

func TestDecodeRefusesUnknownFields(t *testing.T) {
	if _, err := Decode([]byte(`{"kind":"report","owner_epoch":1,"bogus":1}`)); err == nil {
		t.Fatal("unknown field accepted")
	}
	if _, err := Decode([]byte(`{"kind":"report","owner_epoch":1} {}`)); err == nil {
		t.Fatal("trailing data accepted")
	}
}

func TestMergeEntryKeepsTheNewest(t *testing.T) {
	a := entry(1, 10, 9, 3)
	if got := mergeEntry(a, entry(1, 8, 9, 5)); got != entry(1, 10, 9, 5) {
		t.Fatalf("same incarnation = %+v", got)
	}
	if got := mergeEntry(a, entry(2, 3, 1, 1)); got != entry(2, 3, 1, 1) {
		t.Fatalf("newer incarnation = %+v", got)
	}
	if got := mergeEntry(entry(2, 3, 1, 1), a); got != entry(2, 3, 1, 1) {
		t.Fatalf("older incarnation = %+v", got)
	}
}
