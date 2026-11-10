// Package ec2exec is the AWS EC2 host executor for RFC-0001 F01.
// Creates use RunInstances with ClientToken derived from the operation ID.
// Effects stay off until AllowEffects is true and Launch is fully configured.
// Volume effects remain unimplemented (fail closed).
package ec2exec

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/sufuree/sufuree/internal/executor"
)

const (
	tagDeployment = "sufuree:deployment"
	tagOperation  = "sufuree:operation"
	tagManaged    = "sufuree:managed"
)

// API is the EC2 surface this executor needs (for fakes in tests).
type API interface {
	RunInstances(ctx context.Context, params *ec2.RunInstancesInput, optFns ...func(*ec2.Options)) (*ec2.RunInstancesOutput, error)
	TerminateInstances(ctx context.Context, params *ec2.TerminateInstancesInput, optFns ...func(*ec2.Options)) (*ec2.TerminateInstancesOutput, error)
	DescribeInstances(ctx context.Context, params *ec2.DescribeInstancesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
}

// Launch is the approved template for create (AMI, network, shape).
type Launch struct {
	AMI             string
	InstanceType    string
	SubnetID        string
	SecurityGroupID string
	NamePrefix      string // optional Name tag prefix
}

// Config is one EC2 executor.
type Config struct {
	Client        API
	DeploymentUID string
	Launch        Launch
	AllowEffects  bool
	FenceTimeout  time.Duration // Terminate wait; default 10m
	PollInterval  time.Duration // default 5s
}

// Executor runs CreateHost / TerminateHost / Describe against EC2.
type Executor struct {
	cfg Config

	mu   sync.Mutex
	byOp map[string]string // create operation → instance id
}

var _ executor.Executor = (*Executor)(nil)

// New builds an Executor. Client and DeploymentUID are required.
func New(cfg Config) (*Executor, error) {
	if cfg.Client == nil {
		return nil, errors.New("ec2exec: client is required")
	}
	if cfg.DeploymentUID == "" {
		return nil, errors.New("ec2exec: deployment uid is required")
	}
	if cfg.FenceTimeout <= 0 {
		cfg.FenceTimeout = 10 * time.Minute
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	return &Executor{cfg: cfg, byOp: map[string]string{}}, nil
}

// Describe looks up by instance id, create operation id, or operation tag.
func (e *Executor) Describe(ctx context.Context, id string) (executor.HostDesc, error) {
	if id == "" {
		return executor.HostDesc{}, executor.ErrNotFound
	}
	e.mu.Lock()
	if pid, ok := e.byOp[id]; ok {
		id = pid
	}
	e.mu.Unlock()

	out, err := e.cfg.Client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: []string{id},
	})
	if err != nil {
		// Fall back to tag search for operation IDs that are not instance ids.
		return e.describeByOperation(ctx, id)
	}
	inst, ok := firstInstance(out)
	if !ok {
		return e.describeByOperation(ctx, id)
	}
	return descOf(inst), nil
}

func (e *Executor) describeByOperation(ctx context.Context, operationID string) (executor.HostDesc, error) {
	out, err := e.cfg.Client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		Filters: []types.Filter{
			{Name: aws.String("tag:" + tagOperation), Values: []string{operationID}},
			{Name: aws.String("tag:" + tagDeployment), Values: []string{e.cfg.DeploymentUID}},
			{Name: aws.String("instance-state-name"), Values: []string{"pending", "running", "stopping", "stopped", "shutting-down", "terminated"}},
		},
	})
	if err != nil {
		return executor.HostDesc{}, err
	}
	inst, ok := firstInstance(out)
	if !ok {
		return executor.HostDesc{}, executor.ErrNotFound
	}
	return descOf(inst), nil
}

// CreateHost launches one instance when effects are enabled.
func (e *Executor) CreateHost(ctx context.Context, in executor.CreateHostInput) (executor.ProviderRef, error) {
	if !e.cfg.AllowEffects {
		return executor.ProviderRef{}, executor.ErrEffectsDisabled
	}
	if in.OperationID == "" {
		return executor.ProviderRef{}, errors.New("ec2exec: operation id is required")
	}
	if err := e.cfg.Launch.validate(); err != nil {
		return executor.ProviderRef{}, err
	}
	e.mu.Lock()
	if pid, ok := e.byOp[in.OperationID]; ok {
		e.mu.Unlock()
		d, err := e.Describe(ctx, pid)
		if err != nil {
			return executor.ProviderRef{}, err
		}
		return refOf(d), nil
	}
	e.mu.Unlock()

	// Idempotent retry: provider may already have the instance from a timeout.
	if d, err := e.describeByOperation(ctx, in.OperationID); err == nil && d.ProviderID != "" {
		e.remember(in.OperationID, d.ProviderID)
		return refOf(d), nil
	}

	token := clientToken(in.OperationID)
	tags := []types.Tag{
		{Key: aws.String(tagDeployment), Value: aws.String(e.cfg.DeploymentUID)},
		{Key: aws.String(tagOperation), Value: aws.String(in.OperationID)},
		{Key: aws.String(tagManaged), Value: aws.String("true")},
	}
	if e.cfg.Launch.NamePrefix != "" {
		tags = append(tags, types.Tag{
			Key: aws.String("Name"), Value: aws.String(e.cfg.Launch.NamePrefix + "-" + in.OperationID),
		})
	}
	for k, v := range in.Tags {
		if k == "" {
			continue
		}
		tags = append(tags, types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	shape := e.cfg.Launch.InstanceType
	if in.Shape != "" {
		shape = in.Shape
	}
	out, err := e.cfg.Client.RunInstances(ctx, &ec2.RunInstancesInput{
		ImageId:          aws.String(e.cfg.Launch.AMI),
		InstanceType:     types.InstanceType(shape),
		MinCount:         aws.Int32(1),
		MaxCount:         aws.Int32(1),
		SubnetId:         aws.String(e.cfg.Launch.SubnetID),
		SecurityGroupIds: []string{e.cfg.Launch.SecurityGroupID},
		ClientToken:      aws.String(token),
		TagSpecifications: []types.TagSpecification{{
			ResourceType: types.ResourceTypeInstance,
			Tags:         tags,
		}},
	})
	if err != nil {
		return executor.ProviderRef{}, fmt.Errorf("ec2exec: run instances: %w", err)
	}
	if len(out.Instances) == 0 || aws.ToString(out.Instances[0].InstanceId) == "" {
		return executor.ProviderRef{}, errors.New("ec2exec: run instances returned no instance")
	}
	inst := out.Instances[0]
	pid := aws.ToString(inst.InstanceId)
	e.remember(in.OperationID, pid)
	mem := in.MemoryMiB
	if mem <= 0 {
		mem = 256
	}
	return executor.ProviderRef{
		ProviderID: pid,
		Shape:      shape,
		MemoryMiB:  mem,
		CPU:        in.CPU,
	}, nil
}

// TerminateHost requests termination. Repeating the same OperationID is OK.
func (e *Executor) TerminateHost(ctx context.Context, in executor.TerminateHostInput) error {
	if !e.cfg.AllowEffects {
		return executor.ErrEffectsDisabled
	}
	if in.ProviderID == "" {
		return errors.New("ec2exec: provider id is required")
	}
	_, err := e.cfg.Client.TerminateInstances(ctx, &ec2.TerminateInstancesInput{
		InstanceIds: []string{in.ProviderID},
	})
	if err != nil {
		return fmt.Errorf("ec2exec: terminate: %w", err)
	}
	return nil
}

// Fence terminates the exact instance and waits until it is terminated.
func (e *Executor) Fence(ctx context.Context, providerID string) error {
	if !e.cfg.AllowEffects {
		return executor.ErrEffectsDisabled
	}
	if providerID == "" {
		return errors.New("ec2exec: provider id is required")
	}
	if err := e.TerminateHost(ctx, executor.TerminateHostInput{
		OperationID: "fence-" + providerID,
		ProviderID:  providerID,
	}); err != nil {
		return err
	}
	deadline := time.Now().Add(e.cfg.FenceTimeout)
	for {
		d, err := e.Describe(ctx, providerID)
		if err != nil {
			if errors.Is(err, executor.ErrNotFound) {
				return nil
			}
			return err
		}
		if d.State == "terminated" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("ec2exec: fence %s: still %q after %s", providerID, d.State, e.cfg.FenceTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(e.cfg.PollInterval):
		}
	}
}

// AttachVolume is not implemented.
func (e *Executor) AttachVolume(context.Context, executor.AttachVolumeInput) error {
	return fmt.Errorf("ec2exec: attach_volume %w", executor.ErrNotImplemented)
}

// DetachVolume is not implemented.
func (e *Executor) DetachVolume(context.Context, executor.DetachVolumeInput) error {
	return fmt.Errorf("ec2exec: detach_volume %w", executor.ErrNotImplemented)
}

// CreateSnapshot is not implemented.
func (e *Executor) CreateSnapshot(context.Context, executor.SnapshotInput) error {
	return fmt.Errorf("ec2exec: create_snapshot %w", executor.ErrNotImplemented)
}

// RestoreSnapshot is not implemented.
func (e *Executor) RestoreSnapshot(context.Context, executor.RestoreInput) error {
	return fmt.Errorf("ec2exec: restore_snapshot %w", executor.ErrNotImplemented)
}

// ExpireSnapshot is not implemented.
func (e *Executor) ExpireSnapshot(context.Context, executor.ExpireInput) error {
	return fmt.Errorf("ec2exec: expire_snapshot %w", executor.ErrNotImplemented)
}

func (e *Executor) remember(op, pid string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.byOp == nil {
		e.byOp = map[string]string{}
	}
	e.byOp[op] = pid
}

func (l Launch) validate() error {
	if l.AMI == "" || l.InstanceType == "" || l.SubnetID == "" || l.SecurityGroupID == "" {
		return errors.New("ec2exec: launch requires ami, instance type, subnet and security group")
	}
	return nil
}

func clientToken(operationID string) string {
	var b strings.Builder
	for _, r := range operationID {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || strings.ContainsRune("+=./-", r) {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	s := b.String()
	if s == "" {
		s = "op"
	}
	if len(s) > 64 {
		s = s[:64]
	}
	return s
}

func firstInstance(out *ec2.DescribeInstancesOutput) (types.Instance, bool) {
	if out == nil {
		return types.Instance{}, false
	}
	for _, r := range out.Reservations {
		for _, inst := range r.Instances {
			if aws.ToString(inst.InstanceId) != "" {
				return inst, true
			}
		}
	}
	return types.Instance{}, false
}

func descOf(inst types.Instance) executor.HostDesc {
	state := ""
	if inst.State != nil {
		state = string(inst.State.Name)
	}
	op := ""
	for _, t := range inst.Tags {
		if aws.ToString(t.Key) == tagOperation {
			op = aws.ToString(t.Value)
		}
	}
	return executor.HostDesc{
		ID:          aws.ToString(inst.InstanceId),
		ProviderID:  aws.ToString(inst.InstanceId),
		Shape:       string(inst.InstanceType),
		State:       state,
		OperationID: op,
	}
}

func refOf(d executor.HostDesc) executor.ProviderRef {
	return executor.ProviderRef{
		ProviderID: d.ProviderID,
		Shape:      d.Shape,
		MemoryMiB:  d.MemoryMiB,
		CPU:        d.CPU,
	}
}
