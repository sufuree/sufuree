package qual_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"

	"github.com/sufuree/sufuree/internal/budget"
	"github.com/sufuree/sufuree/internal/executor"
	"github.com/sufuree/sufuree/internal/executor/ec2exec"
	"github.com/sufuree/sufuree/internal/qual/ec2live"
)

// TestLiveEC2ZeroOneZero is RFC sequence §3 against real EC2 under a 1-host
// growth grant. Skipped unless SUFUREE_QUAL_EC2=1. Creates one instance,
// fences it, and requires the ledger back at zero. Cleanup fences any
// create that succeeded even if later assertions fail.
func TestLiveEC2ZeroOneZero(t *testing.T) {
	if !ec2live.Enabled() {
		t.Skip("set SUFUREE_QUAL_EC2=1 to run live EC2 0↔1 (real AWS spend)")
	}
	pin, err := ec2live.Resolve()
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(pin.Region))
	if err != nil {
		t.Fatal(err)
	}
	deployment := "qual-live-ec2-" + time.Now().UTC().Format("20060102-150405")
	inner, err := ec2exec.New(ec2exec.Config{
		Client:        ec2.NewFromConfig(cfg),
		DeploymentUID: deployment,
		AllowEffects:  true,
		FenceTimeout:  15 * time.Minute,
		PollInterval:  5 * time.Second,
		Launch: ec2exec.Launch{
			AMI: pin.AMI, InstanceType: pin.InstanceType,
			SubnetID: pin.SubnetID, SecurityGroupID: pin.SecurityGroupID,
			NamePrefix: "sufuree-" + deployment,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger := &budget.Ledger{
		Grant: budget.Grant{
			DeploymentUID: deployment,
			MaxHosts:      pin.GrantMaxHosts,
			MaxMemoryMiB:  pin.GrantMaxMemoryMiB,
			Expires:       time.Now().UTC().Add(2 * time.Hour),
			Revision:      "live-0-1",
		},
	}
	gated := &executor.Gated{Inner: inner, Ledger: ledger}

	createOp := "create-" + deployment
	t0 := time.Now()
	ref, err := gated.CreateHost(ctx, executor.CreateHostInput{
		OperationID: createOp,
		MemoryMiB:   pin.MemoryMiB,
		Shape:       pin.InstanceType,
	})
	createMS := time.Since(t0).Milliseconds()
	if err != nil {
		t.Fatalf("CreateHost: %v", err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()
		_ = gated.TerminateHost(cctx, executor.TerminateHostInput{
			OperationID: "cleanup-" + deployment,
			ProviderID:  ref.ProviderID,
		})
	})

	desc, err := gated.Describe(ctx, ref.ProviderID)
	if err != nil {
		t.Fatalf("Describe after create: %v", err)
	}
	if desc.State != "running" && desc.State != "pending" {
		t.Fatalf("state after create = %q", desc.State)
	}
	h, mem := ledger.Usage()
	if h != 1 || mem != pin.MemoryMiB {
		t.Fatalf("ledger after create hosts=%d mem=%d", h, mem)
	}
	if _, err := gated.CreateHost(ctx, executor.CreateHostInput{
		OperationID: "deny-" + deployment,
		MemoryMiB:   pin.MemoryMiB,
		Shape:       pin.InstanceType,
	}); !errors.Is(err, budget.ErrDenied) {
		t.Fatalf("second create under grant=1: %v", err)
	}

	t1 := time.Now()
	if err := gated.TerminateHost(ctx, executor.TerminateHostInput{
		OperationID: "term-" + deployment,
		ProviderID:  ref.ProviderID,
	}); err != nil {
		t.Fatalf("TerminateHost/Fence: %v", err)
	}
	termMS := time.Since(t1).Milliseconds()

	desc, err = gated.Describe(ctx, ref.ProviderID)
	if err == nil && desc.State != "terminated" {
		t.Fatalf("state after fence = %q", desc.State)
	}
	h, mem = ledger.Usage()
	if h != 0 || mem != 0 {
		t.Fatalf("ledger after terminate hosts=%d mem=%d", h, mem)
	}

	receipt := map[string]any{
		"scenario":           "live-ec2-zero-one-zero",
		"aws_spend":          true,
		"region":             pin.Region,
		"ami":                pin.AMI,
		"ami_name":           pin.AMIName,
		"instance_type":      pin.InstanceType,
		"subnet_id":          pin.SubnetID,
		"security_group_id":  pin.SecurityGroupID,
		"deployment":         deployment,
		"provider_id":        ref.ProviderID,
		"create_operation":   createOp,
		"memory_mib":         pin.MemoryMiB,
		"grant_max_hosts":    pin.GrantMaxHosts,
		"create_ms":          createMS,
		"terminate_fence_ms": termMS,
		"qualified_at":       time.Now().UTC().Format(time.RFC3339),
	}
	if dir := os.Getenv("SUFUREE_QUAL_RECEIPT_DIR"); dir != "" {
		writeLiveEC2Receipt(t, dir, receipt)
	}
}

func writeLiveEC2Receipt(t *testing.T, dir string, receipt map[string]any) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "live-ec2-zero-one-zero.json")
	buf, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(buf, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
