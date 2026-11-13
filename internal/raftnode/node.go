// Package raftnode is one voter for Sufuree control state, built on the
// deterministic github.com/sufuree/raft library (driver + WAL + authenticated
// transport). See docs/adr/0002-control-plane-sufuree-raft.md.
//
// A directory is either created by an explicit bootstrap or opened from
// existing WAL state. An empty directory with Bootstrap false is an error and
// creates nothing. A second bootstrap of a directory that already has state is
// an error and deletes nothing. Legacy HashiCorp Raft state (raft-log.db or
// snapshots/) is refused explicitly: the on-disk format changed and an operator
// must migrate, never silently re-bootstrap over old authority.
package raftnode

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	raft "github.com/sufuree/raft"
	"github.com/sufuree/raft/driver"
	"github.com/sufuree/raft/transport"
	"github.com/sufuree/raft/wal"
)

const (
	walFile        = "control.wal"
	stagingDir     = "staging"
	legacyLogFile  = "raft-log.db"
	legacySnapDir  = "snapshots"
	leaderPollTick = 25 * time.Millisecond

	// The control journal proposes whole-store archives (controlraft caps them
	// at 32 MiB). The command limit must carry that plus the JSON envelope, and
	// the batch limit must carry the command plus framing (raft.FramingReserve).
	maxCommandBytes  = 33 << 20
	maxBatchBytes    = 34 << 20
	maxSnapshotBytes = 96 << 20
)

var (
	// ErrNotLeader means the node is not the leader, so Apply did not propose a command.
	ErrNotLeader = errors.New("raftnode: not leader")
	// ErrExistingState means Bootstrap is set and raft state is already stored.
	ErrExistingState = errors.New("raftnode: raft state already exists")
	// ErrRecoveryRequired means Bootstrap is false and the directory has no raft state.
	// Open does not create a cluster.
	ErrRecoveryRequired = errors.New("raftnode: no raft state; recovery or an explicit bootstrap is required")
	// ErrLegacyState means the directory holds HashiCorp Raft state from before the
	// library swap. It is refused, not migrated or overwritten.
	ErrLegacyState = errors.New("raftnode: legacy HashiCorp raft state present; explicit migration required")
)

// Config is the on-disk identity of one voter.
// DataDir holds control.wal and staging/. The controller's record file stays
// outside this directory.
type Config struct {
	DataDir   string
	NodeID    string
	BindAddr  string
	Bootstrap bool
	// ClusterID binds the WAL and transport to one cluster incarnation. Zero
	// selects 1; use DeriveClusterID to bind it to a deployment UID.
	ClusterID uint64
	// TLS, when set, runs the peer transport over mutually authenticated TLS
	// (controlauth.RaftTransportTLS builds it). Nil generates an ephemeral
	// self-signed certificate for development and tests only.
	TLS *tls.Config
	// OnStore and OnApply observe storage/apply latency for metrics. They must
	// not block; see driver.Config.
	OnStore func(op string, d time.Duration)
	OnApply func(entries int, d time.Duration)
}

func (c Config) validate() error {
	if c.DataDir == "" {
		return errors.New("raftnode: data dir is required")
	}
	if c.NodeID == "" {
		return errors.New("raftnode: node id is required")
	}
	if c.BindAddr == "" {
		return errors.New("raftnode: bind address is required")
	}
	return nil
}

// Node is one Raft voter. Close shuts it down and releases the files and port.
type Node struct {
	drv   *driver.Driver
	store *wal.WAL
	dev   *wal.FileDevice
	tr    *transport.Transport

	leaderCh   chan bool
	stopLeader chan struct{}
	leaderWG   sync.WaitGroup

	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

// Open builds a voter from cfg and fsm.
//
// Bootstrap creates the initial one-voter configuration only when the directory
// has no WAL state. A restart passes Bootstrap false and opens the existing
// store. The WAL keeps synchronization on (file Sync per batch). The
// create/restart discipline mirrors github.com/sufuree/raft/bootstrap: explicit
// creation refuses existing state, and ordinary restart never creates state.
func Open(cfg Config, fsm driver.FSM) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if fsm == nil {
		return nil, errors.New("raftnode: fsm is required")
	}
	if err := refuseLegacy(cfg.DataDir); err != nil {
		return nil, err
	}
	walPath := filepath.Join(cfg.DataDir, walFile)
	_, statErr := os.Stat(walPath)
	walExists := statErr == nil
	if statErr != nil && !os.IsNotExist(statErr) {
		return nil, statErr
	}
	if !cfg.Bootstrap && !walExists {
		return nil, ErrRecoveryRequired
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(cfg.DataDir, 0o700); err != nil {
		return nil, err
	}

	nodeID := MapNodeID(cfg.NodeID)
	clusterID := cfg.ClusterID
	if clusterID == 0 {
		clusterID = 1
	}

	tlsCfg := cfg.TLS
	if tlsCfg == nil {
		var err error
		tlsCfg, err = devTLS()
		if err != nil {
			return nil, fmt.Errorf("raftnode: dev tls: %w", err)
		}
	}
	tr, err := transport.New(transport.Config{
		ID:        nodeID,
		ClusterID: clusterID,
		Listen:    cfg.BindAddr,
		TLS:       tlsCfg,
	})
	if err != nil {
		return nil, err
	}

	dev, err := wal.OpenFile(walPath)
	if err != nil {
		_ = tr.Close()
		return nil, err
	}
	cleanup := func() { _ = dev.Close(); _ = tr.Close() }

	has, err := wal.HasState(dev)
	if err != nil {
		cleanup()
		return nil, err
	}
	if cfg.Bootstrap && has {
		cleanup()
		return nil, fmt.Errorf("%w; refusing to bootstrap", ErrExistingState)
	}
	if !cfg.Bootstrap && !has {
		cleanup()
		return nil, fmt.Errorf("%w; store opened without state", ErrRecoveryRequired)
	}

	wstore, err := wal.Open(dev, wal.Options{
		ClusterID:        clusterID,
		MemberID:         uint64(nodeID),
		Generation:       1,
		MaxBatchBytes:    maxBatchBytes,
		MaxSnapshotBytes: maxSnapshotBytes,
		InitialConfig:    raft.Configuration{Voters: raft.NodeSet{nodeID}},
	})
	if err != nil {
		cleanup()
		return nil, err
	}
	if err := restoreFSM(wstore, fsm); err != nil {
		_ = wstore.Close()
		cleanup()
		return nil, fmt.Errorf("raftnode: restore fsm: %w", err)
	}
	drv, err := driver.New(driver.Config{
		ID:               nodeID,
		MaxCommandBytes:  maxCommandBytes,
		MaxBatchBytes:    maxBatchBytes,
		MaxSnapshotBytes: maxSnapshotBytes,
		StagingDir:       filepath.Join(cfg.DataDir, stagingDir),
		OnStore:          cfg.OnStore,
		OnApply:          cfg.OnApply,
	}, wstore, fsm, tr)
	if err != nil {
		_ = wstore.Close()
		cleanup()
		return nil, err
	}
	drv.Start()

	n := &Node{
		drv:        drv,
		store:      wstore,
		dev:        dev,
		tr:         tr,
		leaderCh:   make(chan bool, 1),
		stopLeader: make(chan struct{}),
	}
	n.leaderWG.Add(1)
	go n.pollLeader()
	return n, nil
}

// restoreFSM rebuilds the application state machine from the durable WAL before
// the driver starts. The library resumes dispatch from the persisted applied
// watermark and assumes the FSM is already durable to that index; Sufuree's FSM
// is a separate record file that may be empty on restart, so it must be
// restored from the snapshot plus committed log replay up to AppliedIndex. The
// core then dispatches any remaining (AppliedIndex, commit] entries itself.
func restoreFSM(wstore *wal.WAL, fsm driver.FSM) error {
	init, err := wstore.Load()
	if err != nil {
		return err
	}
	if !init.Snapshot.Empty() {
		if err := fsm.Restore(init.Snapshot, init.Snapshot.Data); err != nil {
			return err
		}
	}
	if init.AppliedIndex > init.Snapshot.Index {
		entries, err := wstore.ReadEntries(init.Snapshot.Index+1, init.AppliedIndex+1)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			if err := fsm.Apply(entries); err != nil {
				return err
			}
		}
	}
	return nil
}

// refuseLegacy fails when the directory still holds HashiCorp Raft state. The
// WAL format is incompatible; bootstrapping or opening over it could silently
// discard committed authority.
func refuseLegacy(dir string) error {
	for _, name := range []string{legacyLogFile, legacySnapDir} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return fmt.Errorf("%w: %s", ErrLegacyState, filepath.Join(dir, name))
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// MapNodeID derives the library's nonzero uint64 member ID from a stable string
// identity. Numeric strings are used directly; anything else is hashed
// deterministically (FNV-1a 64).
func MapNodeID(s string) raft.NodeID {
	if u, err := strconv.ParseUint(s, 10, 64); err == nil && u != 0 {
		return raft.NodeID(u)
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	id := h.Sum64()
	if id == 0 {
		id = 1
	}
	return raft.NodeID(id)
}

// DeriveClusterID binds a cluster incarnation to a deployment UID. An empty
// deployment selects 1 (the single-deployment development default).
func DeriveClusterID(deployment string) uint64 {
	if deployment == "" {
		return 1
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(deployment))
	id := h.Sum64()
	if id == 0 {
		id = 1
	}
	return id
}

// IsLeader reports whether this process is the current leader. A closed node
// is never leader.
func (n *Node) IsLeader() bool {
	return n != nil && n.drv != nil && !n.closed.Load() && n.drv.IsLeader()
}

// LeaderCh receives true when this node becomes leader and false when it loses
// leadership. Signals can be dropped if the receiver is not waiting; IsLeader
// is the check to trust.
func (n *Node) LeaderCh() <-chan bool {
	if n == nil {
		return nil
	}
	return n.leaderCh
}

func (n *Node) pollLeader() {
	defer n.leaderWG.Done()
	tick := time.NewTicker(leaderPollTick)
	defer tick.Stop()
	last := false
	for {
		select {
		case <-n.stopLeader:
			return
		case <-tick.C:
			cur := n.drv.IsLeader()
			if cur != last {
				last = cur
				select {
				case n.leaderCh <- cur:
				default:
				}
			}
		}
	}
}

// Barrier waits until every log entry committed before the call has been
// applied to the local FSM (a linearizable read barrier). Use it before reading
// FSM state to stage a new command.
func (n *Node) Barrier(ctx context.Context, timeout time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n == nil || n.drv == nil || !n.IsLeader() {
		return ErrNotLeader
	}
	if timeout <= 0 {
		return errors.New("raftnode: barrier timeout is required")
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if _, err := n.drv.Read(cctx); err != nil {
		return wrapDriver(err)
	}
	return nil
}

// Term is the Raft term reported while this node is leader.
func (n *Node) Term() (uint64, error) {
	if n == nil || n.drv == nil || !n.IsLeader() {
		return 0, ErrNotLeader
	}
	return uint64(n.drv.Term()), nil
}

// Apply proposes cmd to the log. It returns ErrNotLeader when this node is not
// the leader, including after Close. timeout bounds how long the proposal may
// wait. An unknown outcome is returned as an error wrapping the library's
// ErrOutcomeUnknown: the command may still commit, so the caller reconciles by
// its own operation identity rather than assuming rollback.
func (n *Node) Apply(ctx context.Context, cmd []byte, timeout time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n == nil || n.drv == nil || !n.IsLeader() {
		return ErrNotLeader
	}
	if timeout <= 0 {
		return errors.New("raftnode: apply timeout is required")
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	res, err := n.drv.Propose(cctx, cmd)
	if err != nil {
		return wrapDriver(err)
	}
	if res.Outcome != raft.OutcomeApplied {
		return fmt.Errorf("raftnode: proposal outcome %v", res.Outcome)
	}
	return nil
}

func wrapDriver(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, driver.ErrNotLeader), errors.Is(err, driver.ErrStopped), errors.Is(err, driver.ErrFailed):
		return fmt.Errorf("%w: %v", ErrNotLeader, err)
	default:
		return err
	}
}

// Snapshot requests an asynchronous snapshot capture through the event loop.
func (n *Node) Snapshot() {
	if n != nil && n.drv != nil {
		n.drv.Snapshot()
	}
}

// SnapshotMeta reports the durable snapshot position (zero when none exists).
func (n *Node) SnapshotMeta() (raft.SnapshotMeta, error) {
	if n == nil || n.store == nil {
		return raft.SnapshotMeta{}, errors.New("raftnode: nil node")
	}
	return n.store.ReadSnapshot()
}

// Watermarks reports the five progress watermarks for metrics.
func (n *Node) Watermarks() (durable, commit, persisted, dispatched, applied uint64) {
	if n == nil || n.drv == nil {
		return 0, 0, 0, 0, 0
	}
	d, c, p, di, a := n.drv.Watermarks()
	return uint64(d), uint64(c), uint64(p), uint64(di), uint64(a)
}

// Close shuts the driver down, releases the port, and closes the WAL and device.
// A second call returns the same error.
func (n *Node) Close() error {
	if n == nil {
		return nil
	}
	n.closeOnce.Do(func() {
		n.closed.Store(true)
		close(n.stopLeader)
		n.leaderWG.Wait()
		var err error
		if n.drv != nil {
			if e := n.drv.Stop(); e != nil && err == nil {
				err = e
			}
		}
		if n.store != nil {
			if e := n.store.Close(); e != nil && err == nil {
				err = e
			}
		}
		if n.dev != nil {
			if e := n.dev.Close(); e != nil && err == nil {
				err = e
			}
		}
		n.closeErr = err
	})
	return n.closeErr
}
