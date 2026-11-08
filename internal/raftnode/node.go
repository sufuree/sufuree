// Package raftnode is one HashiCorp Raft voter for Sufuree control state.
//
// A directory is either created by an explicit bootstrap or opened from
// existing raft state. An empty directory with Bootstrap false is an error
// and creates nothing. A second bootstrap of a directory that already has a
// log, term, or snapshot is an error and deletes nothing. The same Open,
// Apply, and FSM shape is what a later three-voter configuration would use;
// this package does not add or remove voters.
package raftnode

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.etcd.io/bbolt"
)

const (
	logFile        = "raft-log.db"
	snapshotDir    = "snapshots"
	snapshotRetain = 3
)

var (
	// ErrNotLeader means the node is not the leader, so Apply did not propose a command.
	ErrNotLeader = errors.New("raftnode: not leader")
	// ErrExistingState means Bootstrap is set and raft state is already stored.
	ErrExistingState = errors.New("raftnode: raft state already exists")
	// ErrRecoveryRequired means Bootstrap is false and the directory has no raft state.
	// Open does not create a cluster.
	ErrRecoveryRequired = errors.New("raftnode: no raft state; recovery or an explicit bootstrap is required")
)

// Config is the on-disk identity of one voter.
// DataDir holds raft-log.db and snapshots/. The controller's record file
// stays outside this directory.
type Config struct {
	DataDir   string
	NodeID    string
	BindAddr  string
	Bootstrap bool
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
	raft      *raft.Raft
	logs      *raftboltdb.BoltStore
	transport *raft.NetworkTransport

	closeOnce sync.Once
	closeErr  error
}

// Open builds a voter from cfg and fsm.
//
// Bootstrap creates the initial one-voter configuration only when the
// directory has no raft log, term, or snapshot. A restart passes Bootstrap
// false and uses the stores already in DataDir. Bolt synchronization stays
// on: both the raft-boltdb option and the bbolt option set NoSync false.
func Open(cfg Config, fsm raft.FSM) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if fsm == nil {
		return nil, errors.New("raftnode: fsm is required")
	}
	present, err := stateFilesPresent(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	if !cfg.Bootstrap && !present {
		return nil, ErrRecoveryRequired
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(cfg.DataDir, 0o700); err != nil {
		return nil, err
	}

	n := &Node{}
	opened := false
	defer func() {
		if !opened {
			_ = n.Close()
		}
	}()

	logs, err := openBolt(filepath.Join(cfg.DataDir, logFile))
	if err != nil {
		return nil, err
	}
	n.logs = logs

	logger := hclog.New(&hclog.LoggerOptions{
		Name:  "raftnode",
		Level: hclog.Warn,
	})
	snaps, err := raft.NewFileSnapshotStoreWithLogger(cfg.DataDir, snapshotRetain, logger)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(filepath.Join(cfg.DataDir, snapshotDir), 0o700); err != nil {
		return nil, err
	}

	has, err := raft.HasExistingState(logs, logs, snaps)
	if err != nil {
		return nil, err
	}
	if cfg.Bootstrap && has {
		return nil, fmt.Errorf("%w; refusing to bootstrap", ErrExistingState)
	}
	if !cfg.Bootstrap && !has {
		return nil, fmt.Errorf("%w; store opened without a term, log, or snapshot", ErrRecoveryRequired)
	}

	transport, err := raft.NewTCPTransportWithLogger(cfg.BindAddr, nil, 3, 10*time.Second, logger)
	if err != nil {
		return nil, err
	}
	n.transport = transport

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)
	rc.Logger = logger
	r, err := raft.NewRaft(rc, fsm, logs, logs, snaps, transport)
	if err != nil {
		return nil, err
	}
	n.raft = r
	if cfg.Bootstrap {
		future := r.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{{
				ID:       raft.ServerID(cfg.NodeID),
				Address:  transport.LocalAddr(),
				Suffrage: raft.Voter,
			}},
		})
		if err := future.Error(); err != nil {
			if errors.Is(err, raft.ErrCantBootstrap) {
				return nil, fmt.Errorf("%w: %v", ErrExistingState, err)
			}
			return nil, fmt.Errorf("raftnode: bootstrap: %w", err)
		}
	}
	opened = true
	return n, nil
}

// openBolt opens a raft-boltdb file with fsync left on.
func openBolt(path string) (*raftboltdb.BoltStore, error) {
	return raftboltdb.New(raftboltdb.Options{
		Path: path,
		BoltOptions: &bbolt.Options{
			Timeout: time.Second,
			NoSync:  false,
		},
		NoSync: false,
	})
}

// stateFilesPresent reports whether dir already contains a raft log or snapshot.
// A missing directory is not an error and is not created.
func stateFilesPresent(dir string) (bool, error) {
	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if !info.IsDir() {
		return false, fmt.Errorf("raftnode: %s is not a directory", dir)
	}
	logInfo, err := os.Stat(filepath.Join(dir, logFile))
	if err == nil {
		if logInfo.IsDir() {
			return false, errors.New("raftnode: raft-log.db is a directory")
		}
		return true, nil
	}
	if !os.IsNotExist(err) {
		return false, err
	}
	entries, err := os.ReadDir(filepath.Join(dir, snapshotDir))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return len(entries) > 0, nil
}

// Raft returns the underlying Raft instance.
func (n *Node) Raft() *raft.Raft {
	if n == nil {
		return nil
	}
	return n.raft
}

// IsLeader reports whether this process is the current leader.
func (n *Node) IsLeader() bool {
	return n != nil && n.raft != nil && n.raft.State() == raft.Leader
}

// LeaderCh receives true when this node becomes leader and false when it
// loses leadership. Signals can be dropped if the receiver is not waiting;
// IsLeader is the check to trust.
func (n *Node) LeaderCh() <-chan bool {
	if n == nil || n.raft == nil {
		return nil
	}
	return n.raft.LeaderCh()
}

// Apply proposes cmd to the log. It returns ErrNotLeader when this node is
// not the leader, including after Close. timeout bounds how long the proposal
// may wait to be enqueued. A command the FSM rejects is returned after the
// log entry is committed.
func (n *Node) Apply(ctx context.Context, cmd []byte, timeout time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n == nil || n.raft == nil || n.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	if timeout <= 0 {
		return errors.New("raftnode: apply timeout is required")
	}
	future := n.raft.Apply(cmd, timeout)
	errc := make(chan error, 1)
	go func() {
		err := future.Error()
		if err == nil {
			if resp, ok := future.Response().(error); ok {
				err = resp
			}
		}
		errc <- err
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errc:
		return wrapLeader(err)
	}
}

func wrapLeader(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) || errors.Is(err, raft.ErrRaftShutdown) {
		return fmt.Errorf("%w: %v", ErrNotLeader, err)
	}
	return err
}

// Close shuts Raft down, releases the port, and closes the bolt file.
// A second call returns the same error.
func (n *Node) Close() error {
	if n == nil {
		return nil
	}
	n.closeOnce.Do(func() {
		n.closeErr = n.shutdown()
	})
	return n.closeErr
}

func (n *Node) shutdown() error {
	var err error
	if n.raft != nil {
		if e := n.raft.Shutdown().Error(); e != nil && err == nil {
			err = e
		}
	}
	if n.transport != nil {
		if e := n.transport.Close(); e != nil && err == nil {
			err = e
		}
	}
	if n.logs != nil {
		if e := n.logs.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}
