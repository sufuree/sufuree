package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sufuree/sufuree/internal/executor"
	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

func (c *Controller) resume(ctx context.Context, w world) error {
	for _, op := range w.ops {
		if !op.Open() {
			continue
		}
		var err error
		switch {
		case isCreate(op.Action) && op.Outcome == resource.OutcomeUnknown:
			err = c.adoptCreate(ctx, op)
		case isCreate(op.Action):
			err = c.dispatchCreate(ctx, op)
		case isTerminate(op.Action) && op.Outcome == resource.OutcomeUnknown:
			err = nil
		case isTerminate(op.Action):
			err = c.dispatchTerminate(ctx, op)
		default:
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) startCreate(ctx context.Context, pool resource.Resource, memory int) error {
	extra, err := entropy()
	if err != nil {
		return err
	}
	op, err := c.prepare(pool, actionCreate, fmt.Sprintf("c%d-%s", memory, extra), digest(actionCreate, pool.UID, fmt.Sprint(memory), extra))
	if err != nil {
		return err
	}
	return c.dispatchCreate(ctx, op)
}

func (c *Controller) startTerminate(ctx context.Context, pool resource.Resource, hostID string) error {
	extra, err := entropy()
	if err != nil {
		return err
	}
	op, err := c.prepare(pool, actionTerminate, "t-"+hostID+"-"+extra, digest(actionTerminate, pool.UID, hostID, extra))
	if err != nil {
		return err
	}
	return c.dispatchTerminate(ctx, op)
}

// prepare commits prepared and then dispatched before the caller may invoke
// the executor. A store error returns with no call.
func (c *Controller) prepare(pool resource.Resource, action, id, dig string) (resource.Operation, error) {
	created := c.stamp(time.Time{})
	op := resource.Operation{
		OperationID: id, ResourceUID: pool.UID, OwnerEpoch: 0,
		ExpectedGeneration: pool.Generation, Action: action, InputDigest: dig,
		Outcome: resource.OutcomePrepared, CreatedAt: created, UpdatedAt: created,
	}
	var metaEpoch uint64
	err := c.db.Update(func(tx *store.Tx) error {
		meta, err := tx.GetMeta()
		if err != nil {
			return err
		}
		metaEpoch = meta.OwnerEpoch
		op.OwnerEpoch = meta.OwnerEpoch
		if err := tx.PutOperation(op); err != nil {
			return err
		}
		op.Outcome = resource.OutcomeDispatched
		op.UpdatedAt = c.stamp(op.UpdatedAt)
		return tx.PutOperation(op)
	})
	if err != nil {
		return resource.Operation{}, err
	}
	op.OwnerEpoch = metaEpoch
	return op, nil
}

func (c *Controller) dispatchCreate(ctx context.Context, op resource.Operation) error {
	memory, extra, ok := parseCreate(op.OperationID)
	if !ok || digest(actionCreate, op.ResourceUID, fmt.Sprint(memory), extra) != op.InputDigest {
		return fmt.Errorf("reconcile: operation %s does not match its create binding", op.OperationID)
	}
	op, err := c.ensureDispatched(op)
	if err != nil {
		return err
	}
	ref, callErr := c.exec.CreateHost(ctx, executor.CreateHostInput{
		OperationID: op.OperationID, MemoryMiB: memory,
	})
	if callErr != nil {
		return c.recordOutcome(op, outcomeFor(callErr), "")
	}
	if ref.ProviderID == "" || ref.MemoryMiB < 32 {
		if err := c.recordOutcome(op, resource.OutcomeUnknown, ""); err != nil {
			return err
		}
		return errors.New("reconcile: create host returned no provider identity")
	}
	hostID, err := entropy()
	if err != nil {
		return err
	}
	host := resource.Host{
		UID: "h" + hostID, PoolUID: op.ResourceUID, ProviderID: ref.ProviderID,
		MemoryMiB: ref.MemoryMiB, Ready: true, ObservedAt: c.stamp(time.Time{}),
	}
	err = c.db.Update(func(tx *store.Tx) error {
		if err := tx.PutHost(host); err != nil {
			return err
		}
		op.Outcome = resource.OutcomeConfirmed
		op.UpdatedAt = c.stamp(op.UpdatedAt)
		return tx.PutOperation(op)
	})
	if err != nil {
		if recErr := c.recordOutcome(op, resource.OutcomeUnknown, ""); recErr != nil {
			return errors.Join(err, recErr)
		}
		return err
	}
	return nil
}

func (c *Controller) adoptCreate(ctx context.Context, op resource.Operation) error {
	desc, err := c.exec.Describe(ctx, op.OperationID)
	if errors.Is(err, executor.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if desc.State != "running" || desc.ProviderID == "" || desc.MemoryMiB < 32 {
		return nil
	}
	hostID, err := entropy()
	if err != nil {
		return err
	}
	host := resource.Host{
		UID: "h" + hostID, PoolUID: op.ResourceUID, ProviderID: desc.ProviderID,
		MemoryMiB: desc.MemoryMiB, Ready: true, ObservedAt: c.stamp(time.Time{}),
	}
	return c.db.Update(func(tx *store.Tx) error {
		if _, err := tx.GetHost(host.UID); err == nil {
			return nil
		}
		hosts, err := tx.ListHosts()
		if err != nil {
			return err
		}
		for _, h := range hosts {
			if h.ProviderID == desc.ProviderID {
				op.Outcome = resource.OutcomeConfirmed
				op.UpdatedAt = c.stamp(op.UpdatedAt)
				return tx.PutOperation(op)
			}
		}
		if err := tx.PutHost(host); err != nil {
			return err
		}
		op.Outcome = resource.OutcomeConfirmed
		op.UpdatedAt = c.stamp(op.UpdatedAt)
		return tx.PutOperation(op)
	})
}

func (c *Controller) dispatchTerminate(ctx context.Context, op resource.Operation) error {
	hostID, extra, ok := parseTerminate(op.OperationID)
	if !ok || digest(actionTerminate, op.ResourceUID, hostID, extra) != op.InputDigest {
		return fmt.Errorf("reconcile: operation %s does not match its terminate binding", op.OperationID)
	}
	var providerID string
	err := c.db.View(func(tx *store.Tx) error {
		h, err := tx.GetHost(hostID)
		if err != nil {
			return err
		}
		providerID = h.ProviderID
		return nil
	})
	if err != nil {
		return err
	}
	op, err = c.ensureDispatched(op)
	if err != nil {
		return err
	}
	callErr := c.exec.TerminateHost(ctx, executor.TerminateHostInput{
		OperationID: op.OperationID, ProviderID: providerID, HostID: hostID,
	})
	if callErr != nil {
		if errors.Is(callErr, executor.ErrNotFound) {
			missing, derr := c.exec.Describe(ctx, providerID)
			if errors.Is(derr, executor.ErrNotFound) || (derr == nil && missing.State == "terminated") {
				return c.finishTerminate(op, hostID, resource.OutcomeFailed)
			}
		}
		return c.recordOutcome(op, outcomeFor(callErr), "")
	}
	return c.finishTerminate(op, hostID, resource.OutcomeConfirmed)
}

func (c *Controller) finishTerminate(op resource.Operation, hostID string, outcome resource.Outcome) error {
	return c.db.Update(func(tx *store.Tx) error {
		op.Outcome = outcome
		op.UpdatedAt = c.stamp(op.UpdatedAt)
		if err := tx.PutOperation(op); err != nil {
			return err
		}
		if outcome != resource.OutcomeConfirmed && outcome != resource.OutcomeFailed {
			return nil
		}
		if _, err := tx.GetHost(hostID); errors.Is(err, store.ErrNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		return tx.DeleteHost(hostID)
	})
}

func (c *Controller) ensureDispatched(op resource.Operation) (resource.Operation, error) {
	if op.Outcome == resource.OutcomeDispatched {
		return op, nil
	}
	if op.Outcome != resource.OutcomePrepared {
		return op, fmt.Errorf("reconcile: operation %s is %s", op.OperationID, op.Outcome)
	}
	op.Outcome = resource.OutcomeDispatched
	op.UpdatedAt = c.stamp(op.UpdatedAt)
	err := c.db.Update(func(tx *store.Tx) error { return tx.PutOperation(op) })
	return op, err
}

func (c *Controller) recordOutcome(op resource.Operation, outcome resource.Outcome, _ string) error {
	if outcome == "" {
		return errors.New("reconcile: empty outcome")
	}
	op.Outcome = outcome
	op.UpdatedAt = c.stamp(op.UpdatedAt)
	return c.db.Update(func(tx *store.Tx) error { return tx.PutOperation(op) })
}

func outcomeFor(err error) resource.Outcome {
	switch {
	case err == nil:
		return resource.OutcomeConfirmed
	case errors.Is(err, context.DeadlineExceeded):
		return resource.OutcomeUnknown
	case errors.Is(err, executor.ErrEffectsDisabled), errors.Is(err, executor.ErrNotImplemented), errors.Is(err, executor.ErrNotFound):
		return resource.OutcomeFailed
	default:
		return resource.OutcomeUnknown
	}
}
