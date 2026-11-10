package demand

import (
	"context"
	"errors"
	"time"

	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

// DB is the control record file (*store.Store) or a Raft journal that
// proposes the same Update/View shape.
type DB interface {
	Update(func(*store.Tx) error) error
	View(func(*store.Tx) error) error
}

// Bolt persists claims in the control store. A stored claim is pending.
// AcknowledgeHandoff and Cancel delete it; the record cannot change in place.
// Placement generation is returned to waiters and is not written onto the claim.
type Bolt struct {
	Store DB
}

// Update runs fn in one store transaction.
func (b Bolt) Update(ctx context.Context, fn func(Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.Store == nil {
		return errors.New("demand: nil store")
	}
	return b.Store.Update(func(tx *store.Tx) error {
		return fn(boltTx{tx: tx})
	})
}

// View runs fn in one read-only transaction.
func (b Bolt) View(ctx context.Context, fn func(Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.Store == nil {
		return errors.New("demand: nil store")
	}
	return b.Store.View(func(tx *store.Tx) error {
		return fn(boltTx{tx: tx})
	})
}

type boltTx struct{ tx *store.Tx }

func (b boltTx) PutClaim(c Claim) error {
	res, err := b.tx.GetResource(c.ResourceUID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if !c.Deadline.After(now) {
		return errors.New("demand: deadline is not in the future")
	}
	return b.tx.PutClaim(resource.ActivationClaim{
		UID:         c.ClaimID,
		ResourceUID: c.ResourceUID,
		Generation:  res.Generation,
		Count:       1,
		CreatedAt:   now,
		Deadline:    c.Deadline.UTC(),
	})
}

func (b boltTx) GetClaim(id string) (Claim, error) {
	c, err := b.tx.GetClaim(id)
	if errors.Is(err, store.ErrNotFound) {
		return Claim{}, ErrNotFound
	}
	if err != nil {
		return Claim{}, err
	}
	return fromActivation(c), nil
}

func (b boltTx) ListClaims() ([]Claim, error) {
	all, err := b.tx.ListClaims()
	if err != nil {
		return nil, err
	}
	out := make([]Claim, 0, len(all))
	for _, c := range all {
		out = append(out, fromActivation(c))
	}
	return out, nil
}

func (b boltTx) DeleteClaim(id string) error {
	err := b.tx.DeleteClaim(id)
	if errors.Is(err, store.ErrNotFound) {
		return ErrNotFound
	}
	return err
}

func fromActivation(c resource.ActivationClaim) Claim {
	return Claim{
		ClaimID:     c.UID,
		ResourceUID: c.ResourceUID,
		Deadline:    c.Deadline,
		State:       StatePending,
	}
}
