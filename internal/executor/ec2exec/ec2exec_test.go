package ec2exec

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/sufuree/sufuree/internal/executor"
)

type fakeEC2 struct {
	mu        sync.Mutex
	instances map[string]types.Instance
	tokens    map[string]string // client token → instance id
	runN      int
}

func (f *fakeEC2) RunInstances(_ context.Context, in *ec2.RunInstancesInput, _ ...func(*ec2.Options)) (*ec2.RunInstancesOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.instances == nil {
		f.instances = map[string]types.Instance{}
		f.tokens = map[string]string{}
	}
	tok := aws.ToString(in.ClientToken)
	if pid, ok := f.tokens[tok]; ok {
		return &ec2.RunInstancesOutput{Instances: []types.Instance{f.instances[pid]}}, nil
	}
	f.runN++
	pid := "i-" + tok
	var tags []types.Tag
	if len(in.TagSpecifications) > 0 {
		tags = append(tags, in.TagSpecifications[0].Tags...)
	}
	inst := types.Instance{
		InstanceId:   aws.String(pid),
		InstanceType: in.InstanceType,
		State:        &types.InstanceState{Name: types.InstanceStateNameRunning},
		Tags:         tags,
	}
	f.instances[pid] = inst
	f.tokens[tok] = pid
	return &ec2.RunInstancesOutput{Instances: []types.Instance{inst}}, nil
}

func (f *fakeEC2) TerminateInstances(_ context.Context, in *ec2.TerminateInstancesInput, _ ...func(*ec2.Options)) (*ec2.TerminateInstancesOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range in.InstanceIds {
		inst, ok := f.instances[id]
		if !ok {
			continue
		}
		inst.State = &types.InstanceState{Name: types.InstanceStateNameTerminated}
		f.instances[id] = inst
	}
	return &ec2.TerminateInstancesOutput{}, nil
}

func (f *fakeEC2) DescribeInstances(_ context.Context, in *ec2.DescribeInstancesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var found []types.Instance
	if len(in.InstanceIds) > 0 {
		for _, id := range in.InstanceIds {
			if inst, ok := f.instances[id]; ok {
				found = append(found, inst)
			}
		}
		if len(found) == 0 {
			return &ec2.DescribeInstancesOutput{}, nil
		}
		return &ec2.DescribeInstancesOutput{Reservations: []types.Reservation{{Instances: found}}}, nil
	}
	wantOp := ""
	for _, fl := range in.Filters {
		if aws.ToString(fl.Name) == "tag:"+tagOperation && len(fl.Values) > 0 {
			wantOp = fl.Values[0]
		}
	}
	for _, inst := range f.instances {
		if wantOp == "" {
			found = append(found, inst)
			continue
		}
		for _, t := range inst.Tags {
			if aws.ToString(t.Key) == tagOperation && aws.ToString(t.Value) == wantOp {
				found = append(found, inst)
			}
		}
	}
	if len(found) == 0 {
		return &ec2.DescribeInstancesOutput{}, nil
	}
	return &ec2.DescribeInstancesOutput{Reservations: []types.Reservation{{Instances: found}}}, nil
}

func TestCreateIdempotentAndFence(t *testing.T) {
	fake := &fakeEC2{}
	ex, err := New(Config{
		Client:        fake,
		DeploymentUID: "dep-1",
		AllowEffects:  true,
		FenceTimeout:  time.Second,
		PollInterval:  time.Millisecond,
		Launch: Launch{
			AMI: "ami-1", InstanceType: "t3.micro",
			SubnetID: "subnet-1", SecurityGroupID: "sg-1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := ex.CreateHost(context.Background(), executor.CreateHostInput{
		OperationID: "op-create-1", MemoryMiB: 256, CPU: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	again, err := ex.CreateHost(context.Background(), executor.CreateHostInput{
		OperationID: "op-create-1", MemoryMiB: 256, CPU: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if again.ProviderID != ref.ProviderID || fake.runN != 1 {
		t.Fatalf("ref=%s again=%s runs=%d", ref.ProviderID, again.ProviderID, fake.runN)
	}
	if again.MemoryMiB != 256 {
		t.Fatalf("again.MemoryMiB = %d", again.MemoryMiB)
	}
	d0, err := ex.Describe(context.Background(), ref.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if d0.MemoryMiB != 256 {
		t.Fatalf("describe MemoryMiB = %d", d0.MemoryMiB)
	}
	if _, err := ex.CreateHost(context.Background(), executor.CreateHostInput{
		OperationID: "op-bad-shape", MemoryMiB: 256, Shape: "m5.large",
	}); err == nil {
		t.Fatal("expected unapproved shape rejection")
	}
	if err := ex.Fence(context.Background(), ref.ProviderID); err != nil {
		t.Fatal(err)
	}
	d, err := ex.Describe(context.Background(), ref.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if d.State != "terminated" {
		t.Fatalf("state = %q", d.State)
	}
}

func TestEffectsDisabled(t *testing.T) {
	ex, err := New(Config{
		Client: &fakeEC2{}, DeploymentUID: "d",
		Launch: Launch{AMI: "a", InstanceType: "t", SubnetID: "s", SecurityGroupID: "g"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ex.CreateHost(context.Background(), executor.CreateHostInput{OperationID: "x", MemoryMiB: 128})
	if !errors.Is(err, executor.ErrEffectsDisabled) {
		t.Fatalf("err = %v", err)
	}
}
