// Package store is the durable bbolt record of Sufuree control state.
//
// Writes run in one transaction and return only after Sync. NoSync stays
// false; a write is refused if that changes. A nil error means the
// transaction committed and Sync returned nil. A non-nil error means the
// caller must not treat the change as confirmed: when the transaction
// committed and Sync then failed, a later View shows what was committed.
//
// Resource updates are conditional on ResourceVersion. Cluster meta is
// created and advanced, never deleted, so opening an empty directory cannot
// be mistaken for a fresh deployment by wiping identity here.
package store

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/sufuree/sufuree/internal/resource"
	"go.etcd.io/bbolt"
)

const (
	metaKeyName   = "cluster"
	schemaKeyName = "schema"
	schemaVersion = "1"
)

var (
	bucketResources  = []byte("resources")
	bucketOperations = []byte("operations")
	bucketPlacements = []byte("placements")
	bucketHosts      = []byte("hosts")
	bucketClaims     = []byte("claims")
	bucketMeta       = []byte("meta")
	bucketBudget     = []byte("budget")
	allBuckets       = [][]byte{
		bucketResources, bucketOperations, bucketPlacements, bucketHosts, bucketClaims, bucketMeta,
		bucketRevocations, bucketRevokeQueue, bucketBudget,
	}
)

var (
	// ErrNotFound is a missing record. It is not an empty successful read.
	ErrNotFound = errors.New("store: not found")
	// ErrStaleResourceVersion means the caller's ResourceVersion is not the stored one.
	ErrStaleResourceVersion = errors.New("store: stale resource version")
	// ErrGeneration means a spec change and the spec generation do not move together.
	ErrGeneration = errors.New("store: generation does not match spec change")
	// ErrIdentity means an immutable binding would change.
	ErrIdentity = errors.New("store: identity is immutable")
	// ErrRegression means an epoch, incarnation, or timestamp would move backward.
	ErrRegression = errors.New("store: refused backward meta or timestamp")
	// ErrInUse means a record still has dependent state, or an operation is unfinished.
	ErrInUse = errors.New("store: still referenced")
	// ErrNameTaken means another resource of the same kind already has this name.
	ErrNameTaken = errors.New("store: name is in use")
	// ErrOutcome means the operation outcome transition is not allowed.
	ErrOutcome = errors.New("store: operation outcome transition refused")
	// ErrLifecycle means a deleting resource would change spec or leave deleting.
	ErrLifecycle = errors.New("store: lifecycle forbids this change")
)

// Store is one controller's durable control file.
type Store struct {
	mu     sync.Mutex
	db     *bbolt.DB
	path   string
	closed bool
}

// Open opens path, creating the parent directory if needed. It does not
// invent a cluster identity. The file is created mode 0600. NoSync is false.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("store: path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{
		NoSync:  false,
		Timeout: time.Second,
	})
	if err != nil {
		return nil, err
	}
	if db.NoSync {
		_ = db.Close()
		return nil, errors.New("store: bbolt opened with NoSync enabled")
	}
	s := &Store{db: db, path: path}
	err = s.Update(func(tx *Tx) error {
		for _, name := range allBuckets {
			if _, err := tx.btx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		if err := ensureBootFloorBucket(tx); err != nil {
			return err
		}
		return tx.ensureSchema()
	})
	if err != nil {
		cerr := s.Close()
		if cerr != nil {
			return nil, fmt.Errorf("%w (close: %v)", err, cerr)
		}
		return nil, err
	}
	return s, nil
}

// Close releases the file. A second call returns nil without another sync.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if s.db == nil {
		s.closed = true
		return nil
	}
	err := s.db.Close()
	if err != nil {
		return err
	}
	s.closed = true
	s.db = nil
	return nil
}

// Update runs fn in one writable transaction. fn must not call Update, View,
// or Close. On a nil return from fn, Sync runs before Update returns nil.
func (s *Store) Update(fn func(*Tx) error) error {
	if fn == nil {
		return errors.New("store: nil update")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.db == nil {
		return errors.New("store: closed")
	}
	if s.db.NoSync {
		return errors.New("store: refusing write while NoSync is set")
	}
	err := s.db.Update(func(btx *bbolt.Tx) error {
		return fn(&Tx{btx: btx})
	})
	if err != nil {
		return err
	}
	if s.db.NoSync {
		return errors.New("store: write committed but NoSync is set; durability was not confirmed")
	}
	if err := s.db.Sync(); err != nil {
		return fmt.Errorf("store: sync: %w", err)
	}
	return nil
}

// View runs fn in one read-only transaction. fn must not call Update, View, or Close.
func (s *Store) View(fn func(*Tx) error) error {
	if fn == nil {
		return errors.New("store: nil view")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.db == nil {
		return errors.New("store: closed")
	}
	return s.db.View(func(btx *bbolt.Tx) error {
		return fn(&Tx{btx: btx})
	})
}

// Tx is one bbolt transaction. It is valid only for the Update or View callback.
type Tx struct {
	btx *bbolt.Tx
}

func (tx *Tx) ensureSchema() error {
	b, err := tx.bucket(bucketMeta)
	if err != nil {
		return err
	}
	got := b.Get([]byte(schemaKeyName))
	if got == nil {
		empty, err := tx.emptyData()
		if err != nil {
			return err
		}
		if !empty {
			return errors.New("store: missing schema on a non-empty database")
		}
		return b.Put([]byte(schemaKeyName), []byte(schemaVersion))
	}
	if string(got) != schemaVersion {
		return fmt.Errorf("store: schema %q is not supported", got)
	}
	return nil
}

func (tx *Tx) emptyData() (bool, error) {
	for _, name := range [][]byte{bucketResources, bucketOperations, bucketPlacements, bucketHosts, bucketClaims, bucketRevocations, bucketRevokeQueue, bucketBudget} {
		b, err := tx.bucket(name)
		if err != nil {
			return false, err
		}
		k, _ := b.Cursor().First()
		if k != nil {
			return false, nil
		}
	}
	b, err := tx.bucket(bucketMeta)
	if err != nil {
		return false, err
	}
	if b.Get([]byte(metaKeyName)) != nil {
		return false, nil
	}
	return true, nil
}

// ApplyResource creates or updates a resource if in.ResourceVersion is the
// stored version. A new resource uses version 0 and is stored as version 1.
// Each successful apply stores the next version. The returned record is the
// value read back from the transaction.
//
// Generation stays put when Spec is unchanged and increases by exactly one
// when Spec changes. Entering or remaining in deleting freezes Spec and
// Generation. Kind and name do not change.
func (tx *Tx) ApplyResource(in resource.Resource) (resource.Resource, error) {
	in = resource.Normalize(in)
	if err := checkKey(in.UID); err != nil {
		return resource.Resource{}, err
	}
	prev, err := tx.GetResource(in.UID)
	found := err == nil
	if err != nil && !errors.Is(err, ErrNotFound) {
		return resource.Resource{}, err
	}
	if !found && in.ResourceVersion != 0 {
		return resource.Resource{}, fmt.Errorf("store: resource %q: %w", in.UID, ErrStaleResourceVersion)
	}
	if !found {
		if _, err := tx.GetRevocation(in.UID); err == nil {
			return resource.Resource{}, fmt.Errorf("store: resource %q: %w", in.UID, ErrRevoked)
		} else if !errors.Is(err, ErrNotFound) {
			return resource.Resource{}, err
		}
	}
	if found && prev.ResourceVersion != in.ResourceVersion {
		return resource.Resource{}, fmt.Errorf("store: resource %q: %w", in.UID, ErrStaleResourceVersion)
	}
	if found && prev.ResourceVersion == math.MaxUint64 {
		return resource.Resource{}, errors.New("store: resource version exhausted")
	}
	if err := in.Validate(); err != nil {
		return resource.Resource{}, err
	}
	if found {
		if prev.Kind != in.Kind || prev.Name != in.Name {
			return resource.Resource{}, fmt.Errorf("store: resource %q: %w", in.UID, ErrIdentity)
		}
		specSame := reflect.DeepEqual(prev.Spec, in.Spec)
		switch {
		case prev.Lifecycle == resource.LifecycleDeleting || in.Lifecycle == resource.LifecycleDeleting:
			if in.Lifecycle != resource.LifecycleDeleting || !specSame || in.Generation != prev.Generation {
				return resource.Resource{}, fmt.Errorf("store: resource %q: %w", in.UID, ErrLifecycle)
			}
		case !specSame:
			if prev.Generation == math.MaxUint64 || in.Generation != prev.Generation+1 {
				return resource.Resource{}, fmt.Errorf("store: resource %q: %w", in.UID, ErrGeneration)
			}
		case in.Generation != prev.Generation:
			return resource.Resource{}, fmt.Errorf("store: resource %q: %w", in.UID, ErrGeneration)
		}
	}
	if err := tx.nameFree(in); err != nil {
		return resource.Resource{}, err
	}
	in.ResourceVersion++
	if err := putBucket(tx, bucketResources, in.UID, in); err != nil {
		return resource.Resource{}, err
	}
	return tx.GetResource(in.UID)
}

func (tx *Tx) nameFree(in resource.Resource) error {
	all, err := tx.ListResources()
	if err != nil {
		return err
	}
	for _, other := range all {
		if other.UID != in.UID && other.Kind == in.Kind && other.Name == in.Name {
			return fmt.Errorf("store: %s %q: %w", in.Kind, in.Name, ErrNameTaken)
		}
	}
	return nil
}

// GetResource reads one resource. A missing or corrupt record is an error.
func (tx *Tx) GetResource(uid string) (resource.Resource, error) {
	return getBucket[resource.Resource](tx, bucketResources, uid)
}

// ListResources returns every valid resource in key order.
func (tx *Tx) ListResources() ([]resource.Resource, error) {
	return listBucket[resource.Resource](tx, bucketResources)
}

// DeleteResource removes a resource that has no operations, placements,
// claims, or, for a pool, hosts. It does not release provider resources.
// Deleting an app records its installation identity's revocation in the
// same transaction (Revocation), which also keeps its UID from being used
// again.
func (tx *Tx) DeleteResource(uid string) error {
	res, err := tx.GetResource(uid)
	if err != nil {
		return err
	}
	if err := tx.resourceIdle(res); err != nil {
		return err
	}
	if res.Kind == resource.KindApp {
		if err := tx.revoke(res.UID, res.UID); err != nil {
			return err
		}
	}
	return delBucket(tx, bucketResources, uid)
}

func (tx *Tx) resourceIdle(res resource.Resource) error {
	ops, err := tx.ListOperations()
	if err != nil {
		return err
	}
	for _, op := range ops {
		if op.ResourceUID == res.UID {
			return fmt.Errorf("store: resource %q still has operation %s: %w", res.UID, op.OperationID, ErrInUse)
		}
	}
	placements, err := tx.ListPlacements()
	if err != nil {
		return err
	}
	for _, p := range placements {
		if p.ResourceUID == res.UID {
			return fmt.Errorf("store: resource %q still has placement %s: %w", res.UID, p.UID, ErrInUse)
		}
	}
	claims, err := tx.ListClaims()
	if err != nil {
		return err
	}
	for _, c := range claims {
		if c.ResourceUID == res.UID {
			return fmt.Errorf("store: resource %q still has activation claim %s: %w", res.UID, c.UID, ErrInUse)
		}
	}
	if res.Kind == resource.KindPool {
		hosts, err := tx.ListHosts()
		if err != nil {
			return err
		}
		for _, h := range hosts {
			if h.PoolUID == res.UID {
				return fmt.Errorf("store: resource %q still has host %s: %w", res.UID, h.UID, ErrInUse)
			}
		}
	}
	return nil
}

// PutOperation creates an operation as prepared, or records a later outcome.
// The action, resource, epoch, generation, digest, and created time are
// fixed on create. Create checks the current cluster owner epoch and the
// resource generation. Later outcomes do not rewrite that binding. A lost
// reply is recorded as unknown, not confirmed.
func (tx *Tx) PutOperation(op resource.Operation) error {
	if err := op.Validate(); err != nil {
		return err
	}
	old, err := tx.GetOperation(op.OperationID)
	if errors.Is(err, ErrNotFound) {
		if op.Outcome != resource.OutcomePrepared {
			return fmt.Errorf("store: operation %q must be created as prepared: %w", op.OperationID, ErrOutcome)
		}
		meta, err := tx.GetMeta()
		if err != nil {
			return err
		}
		if op.OwnerEpoch != meta.OwnerEpoch {
			return fmt.Errorf("store: operation %q owner epoch %d does not match cluster epoch %d: %w", op.OperationID, op.OwnerEpoch, meta.OwnerEpoch, ErrRegression)
		}
		res, err := tx.GetResource(op.ResourceUID)
		if err != nil {
			return err
		}
		if op.ExpectedGeneration != res.Generation {
			return fmt.Errorf("store: operation %q: %w", op.OperationID, ErrGeneration)
		}
		return putBucket(tx, bucketOperations, op.OperationID, op)
	}
	if err != nil {
		return err
	}
	if !sameOperationBinding(old, op) {
		return fmt.Errorf("store: operation %q: %w", op.OperationID, ErrIdentity)
	}
	if !canAdvance(old.Outcome, op.Outcome) {
		return fmt.Errorf("store: operation %q outcome %q cannot become %q: %w", op.OperationID, old.Outcome, op.Outcome, ErrOutcome)
	}
	if op.UpdatedAt.Before(old.UpdatedAt) {
		return fmt.Errorf("store: operation %q: %w", op.OperationID, ErrRegression)
	}
	if old.Outcome != op.Outcome && !op.UpdatedAt.After(old.UpdatedAt) {
		return fmt.Errorf("store: operation %q outcome change needs a later updated_at: %w", op.OperationID, ErrRegression)
	}
	return putBucket(tx, bucketOperations, op.OperationID, op)
}

func sameOperationBinding(a, b resource.Operation) bool {
	return a.OperationID == b.OperationID &&
		a.ResourceUID == b.ResourceUID &&
		a.OwnerEpoch == b.OwnerEpoch &&
		a.ExpectedGeneration == b.ExpectedGeneration &&
		a.Action == b.Action &&
		a.InputDigest == b.InputDigest &&
		a.CreatedAt.Equal(b.CreatedAt)
}

func canAdvance(from, to resource.Outcome) bool {
	if from == to {
		return true
	}
	switch from {
	case resource.OutcomePrepared:
		return to == resource.OutcomeDispatched || to == resource.OutcomeUnknown || to == resource.OutcomeFailed
	case resource.OutcomeDispatched:
		return to == resource.OutcomeConfirmed || to == resource.OutcomeUnknown || to == resource.OutcomeFailed
	case resource.OutcomeUnknown:
		return to == resource.OutcomeDispatched || to == resource.OutcomeConfirmed || to == resource.OutcomeFailed
	default:
		return false
	}
}

// GetOperation reads one operation.
func (tx *Tx) GetOperation(id string) (resource.Operation, error) {
	return getBucket[resource.Operation](tx, bucketOperations, id)
}

// ListOperations returns every operation in key order.
func (tx *Tx) ListOperations() ([]resource.Operation, error) {
	return listBucket[resource.Operation](tx, bucketOperations)
}

// ListOpenOperations returns prepared, dispatched, and unknown operations.
// confirmed and failed are omitted. A corrupt record fails the call.
func (tx *Tx) ListOpenOperations() ([]resource.Operation, error) {
	all, err := tx.ListOperations()
	if err != nil {
		return nil, err
	}
	out := []resource.Operation{}
	for _, op := range all {
		if op.Open() {
			out = append(out, op)
		}
	}
	return out, nil
}

// DeleteOperation removes a confirmed or failed operation. An open outcome
// stays stored so unfinished work is not dropped.
func (tx *Tx) DeleteOperation(id string) error {
	op, err := tx.GetOperation(id)
	if err != nil {
		return err
	}
	if !op.Terminal() {
		return fmt.Errorf("store: operation %q is %s: %w", id, op.Outcome, ErrInUse)
	}
	return delBucket(tx, bucketOperations, id)
}

// PutPlacement creates a placement or updates its phase, owner epoch, and
// timestamp. The assignment identity does not change in place.
func (tx *Tx) PutPlacement(p resource.Placement) error {
	if err := p.Validate(); err != nil {
		return err
	}
	old, err := tx.GetPlacement(p.UID)
	if errors.Is(err, ErrNotFound) {
		if _, err := tx.GetResource(p.ResourceUID); err != nil {
			return err
		}
		if _, err := tx.GetHost(p.HostUID); err != nil {
			return err
		}
		return putBucket(tx, bucketPlacements, p.UID, p)
	}
	if err != nil {
		return err
	}
	if old.ResourceUID != p.ResourceUID || old.HostUID != p.HostUID || old.Replica != p.Replica || old.VolumeUID != p.VolumeUID || old.Generation != p.Generation || old.MemoryMiB != p.MemoryMiB {
		return fmt.Errorf("store: placement %q: %w", p.UID, ErrIdentity)
	}
	if p.OwnerEpoch < old.OwnerEpoch || p.UpdatedAt.Before(old.UpdatedAt) {
		return fmt.Errorf("store: placement %q: %w", p.UID, ErrRegression)
	}
	return putBucket(tx, bucketPlacements, p.UID, p)
}

// GetPlacement reads one placement.
func (tx *Tx) GetPlacement(uid string) (resource.Placement, error) {
	return getBucket[resource.Placement](tx, bucketPlacements, uid)
}

// ListPlacements returns every placement in key order.
func (tx *Tx) ListPlacements() ([]resource.Placement, error) {
	return listBucket[resource.Placement](tx, bucketPlacements)
}

// DeletePlacement removes the placement record. It does not stop a guest.
// When the placement held the last copy of a one-off replica ID, that
// installation identity's revocation is recorded in the same transaction.
func (tx *Tx) DeletePlacement(uid string) error {
	p, err := tx.GetPlacement(uid)
	if err != nil {
		return err
	}
	if err := delBucket(tx, bucketPlacements, uid); err != nil {
		return err
	}
	return tx.releasedReplica(p)
}

// PutHost creates a host in an existing pool, or updates readiness,
// draining, and the observation time. Provider identity and memory stay put.
func (tx *Tx) PutHost(h resource.Host) error {
	if err := h.Validate(); err != nil {
		return err
	}
	old, err := tx.GetHost(h.UID)
	if errors.Is(err, ErrNotFound) {
		pool, err := tx.GetResource(h.PoolUID)
		if err != nil {
			return err
		}
		if pool.Kind != resource.KindPool {
			return fmt.Errorf("store: host %q: pool %q is not a pool", h.UID, h.PoolUID)
		}
		return putBucket(tx, bucketHosts, h.UID, h)
	}
	if err != nil {
		return err
	}
	if old.PoolUID != h.PoolUID || old.ProviderID != h.ProviderID || old.MemoryMiB != h.MemoryMiB {
		return fmt.Errorf("store: host %q: %w", h.UID, ErrIdentity)
	}
	if h.ObservedAt.Before(old.ObservedAt) {
		return fmt.Errorf("store: host %q: %w", h.UID, ErrRegression)
	}
	return putBucket(tx, bucketHosts, h.UID, h)
}

// GetHost reads one host.
func (tx *Tx) GetHost(uid string) (resource.Host, error) {
	return getBucket[resource.Host](tx, bucketHosts, uid)
}

// ListHosts returns every host in key order.
func (tx *Tx) ListHosts() ([]resource.Host, error) {
	return listBucket[resource.Host](tx, bucketHosts)
}

// DeleteHost removes a host that no placement still references.
func (tx *Tx) DeleteHost(uid string) error {
	if _, err := tx.GetHost(uid); err != nil {
		return err
	}
	placements, err := tx.ListPlacements()
	if err != nil {
		return err
	}
	for _, p := range placements {
		if p.HostUID == uid {
			return fmt.Errorf("store: host %q still has placement %s: %w", uid, p.UID, ErrInUse)
		}
	}
	return delBucket(tx, bucketHosts, uid)
}

// PutClaim creates an activation claim for the resource's current generation.
// A repeated identical claim is left as stored. Any other change is refused;
// a new demand is a new claim UID.
func (tx *Tx) PutClaim(c resource.ActivationClaim) error {
	if err := c.Validate(); err != nil {
		return err
	}
	old, err := tx.GetClaim(c.UID)
	if errors.Is(err, ErrNotFound) {
		res, err := tx.GetResource(c.ResourceUID)
		if err != nil {
			return err
		}
		if c.Generation != res.Generation {
			return fmt.Errorf("store: activation claim %q: %w", c.UID, ErrGeneration)
		}
		return putBucket(tx, bucketClaims, c.UID, c)
	}
	if err != nil {
		return err
	}
	if !sameClaim(old, c) {
		return fmt.Errorf("store: activation claim %q: %w", c.UID, ErrIdentity)
	}
	return nil
}

func sameClaim(a, b resource.ActivationClaim) bool {
	return a.UID == b.UID &&
		a.ResourceUID == b.ResourceUID &&
		a.Generation == b.Generation &&
		a.Count == b.Count &&
		a.CreatedAt.Equal(b.CreatedAt) &&
		a.Deadline.Equal(b.Deadline)
}

// GetClaim reads one activation claim.
func (tx *Tx) GetClaim(uid string) (resource.ActivationClaim, error) {
	return getBucket[resource.ActivationClaim](tx, bucketClaims, uid)
}

// ListClaims returns every activation claim in key order.
func (tx *Tx) ListClaims() ([]resource.ActivationClaim, error) {
	return listBucket[resource.ActivationClaim](tx, bucketClaims)
}

// DeleteClaim removes the claim record. It does not cancel guest work.
func (tx *Tx) DeleteClaim(uid string) error {
	if _, err := tx.GetClaim(uid); err != nil {
		return err
	}
	return delBucket(tx, bucketClaims, uid)
}

// PutMeta writes cluster identity. The deployment UID does not change.
// Recovery incarnation and owner epoch do not decrease. A newer incarnation
// also requires a newer owner epoch.
func (tx *Tx) PutMeta(m resource.ClusterMeta) error {
	if err := m.Validate(); err != nil {
		return err
	}
	old, err := tx.GetMeta()
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if err == nil {
		if old.DeploymentUID != m.DeploymentUID {
			return fmt.Errorf("store: cluster meta: %w", ErrIdentity)
		}
		if m.RecoveryIncarnation < old.RecoveryIncarnation || m.OwnerEpoch < old.OwnerEpoch {
			return fmt.Errorf("store: cluster meta: %w", ErrRegression)
		}
		if m.RecoveryIncarnation > old.RecoveryIncarnation && m.OwnerEpoch <= old.OwnerEpoch {
			return fmt.Errorf("store: cluster meta: recovery incarnation requires a newer owner epoch: %w", ErrRegression)
		}
	}
	return putBucket(tx, bucketMeta, metaKeyName, m)
}

// GetMeta reads cluster identity. A new store has none until PutMeta.
func (tx *Tx) GetMeta() (resource.ClusterMeta, error) {
	return getBucket[resource.ClusterMeta](tx, bucketMeta, metaKeyName)
}

// BumpOwnerEpoch adds one to the stored owner epoch and returns the stored meta.
func (tx *Tx) BumpOwnerEpoch() (resource.ClusterMeta, error) {
	m, err := tx.GetMeta()
	if err != nil {
		return resource.ClusterMeta{}, err
	}
	if m.OwnerEpoch == math.MaxUint64 {
		return resource.ClusterMeta{}, errors.New("store: owner epoch exhausted")
	}
	m.OwnerEpoch++
	if err := tx.PutMeta(m); err != nil {
		return resource.ClusterMeta{}, err
	}
	return tx.GetMeta()
}

func (tx *Tx) bucket(name []byte) (*bbolt.Bucket, error) {
	if tx == nil || tx.btx == nil {
		return nil, errors.New("store: nil transaction")
	}
	b := tx.btx.Bucket(name)
	if b == nil {
		return nil, fmt.Errorf("store: bucket %q is missing", name)
	}
	return b, nil
}

func checkKey(key string) error {
	if key == "" {
		return fmt.Errorf("store: empty key: %w", ErrNotFound)
	}
	return nil
}

func putBucket(tx *Tx, name []byte, key string, v any) error {
	if err := checkKey(key); err != nil {
		return err
	}
	b, err := tx.bucket(name)
	if err != nil {
		return err
	}
	buf, err := resource.Encode(v)
	if err != nil {
		return err
	}
	if len(buf) == 0 {
		return errors.New("store: empty record")
	}
	return b.Put([]byte(key), buf)
}

func getBucket[T interface{ Validate() error }](tx *Tx, name []byte, key string) (T, error) {
	var zero T
	if err := checkKey(key); err != nil {
		return zero, fmt.Errorf("store: %s: %w", name, ErrNotFound)
	}
	b, err := tx.bucket(name)
	if err != nil {
		return zero, err
	}
	v := b.Get([]byte(key))
	if v == nil {
		return zero, fmt.Errorf("store: %s %q: %w", name, key, ErrNotFound)
	}
	buf := append([]byte(nil), v...)
	var item T
	if err := resource.Decode(buf, &item); err != nil {
		return zero, fmt.Errorf("store: %s %q: %w", name, key, err)
	}
	if err := item.Validate(); err != nil {
		return zero, fmt.Errorf("store: %s %q: %w", name, key, err)
	}
	return item, nil
}

func listBucket[T interface{ Validate() error }](tx *Tx, name []byte) ([]T, error) {
	b, err := tx.bucket(name)
	if err != nil {
		return nil, err
	}
	out := []T{}
	err = b.ForEach(func(k, v []byte) error {
		buf := append([]byte(nil), v...)
		var item T
		if err := resource.Decode(buf, &item); err != nil {
			return fmt.Errorf("store: %s %q: %w", name, k, err)
		}
		if err := item.Validate(); err != nil {
			return fmt.Errorf("store: %s %q: %w", name, k, err)
		}
		out = append(out, item)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func delBucket(tx *Tx, name []byte, key string) error {
	if err := checkKey(key); err != nil {
		return err
	}
	b, err := tx.bucket(name)
	if err != nil {
		return err
	}
	if b.Get([]byte(key)) == nil {
		return fmt.Errorf("store: %s %q: %w", name, key, ErrNotFound)
	}
	return b.Delete([]byte(key))
}

// PutBudgetReservation durably records one growth-ledger entry (RFC-0001 F01).
// It is keyed by operation ID so a restart re-seeds spent capacity.
func (tx *Tx) PutBudgetReservation(r resource.BudgetReservation) error {
	if err := r.Validate(); err != nil {
		return err
	}
	return putBucket(tx, bucketBudget, r.OperationID, r)
}

// ListBudgetReservations returns every durable growth-ledger entry.
func (tx *Tx) ListBudgetReservations() ([]resource.BudgetReservation, error) {
	return listBucket[resource.BudgetReservation](tx, bucketBudget)
}
