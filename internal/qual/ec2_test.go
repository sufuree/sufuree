package qual_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/sufuree/sufuree/internal/budget"
	"github.com/sufuree/sufuree/internal/executor"
	"github.com/sufuree/sufuree/internal/executor/ec2exec"
)

// TestLiveEC2CreateTerminateFence is the live AWS provider-effect qualification
// for RFC-0001 F01: create under a growth grant, ClientToken idempotency, and
// fence to observed terminal state. It is skipped unless SUFUREE_QUAL_EC2=1 and
// makes REAL, billed EC2 calls (a t3.nano for well under a minute). It always
// terminates the instance and deletes the temporary security group, even on
// failure. See docs/qualification/rfc-0001-cloud-path.md.
//
// Required env: SUFUREE_QUAL_EC2_AMI, SUFUREE_QUAL_EC2_SUBNET.
// Optional: SUFUREE_QUAL_EC2_REGION (default us-west-1),
// SUFUREE_QUAL_EC2_INSTANCE_TYPE (default t3.nano), SUFUREE_QUAL_RECEIPT_DIR.
func TestLiveEC2CreateTerminateFence(t *testing.T) {
	if os.Getenv("SUFUREE_QUAL_EC2") != "1" {
		t.Skip("set SUFUREE_QUAL_EC2=1 with SUFUREE_QUAL_EC2_AMI and SUFUREE_QUAL_EC2_SUBNET to run live EC2 qual (real spend)")
	}
	ami := os.Getenv("SUFUREE_QUAL_EC2_AMI")
	subnet := os.Getenv("SUFUREE_QUAL_EC2_SUBNET")
	if ami == "" || subnet == "" {
		t.Fatal("SUFUREE_QUAL_EC2_AMI and SUFUREE_QUAL_EC2_SUBNET are required")
	}
	region := os.Getenv("SUFUREE_QUAL_EC2_REGION")
	if region == "" {
		region = "us-west-1"
	}
	instType := os.Getenv("SUFUREE_QUAL_EC2_INSTANCE_TYPE")
	if instType == "" {
		instType = "t3.nano"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatal(err)
	}
	client := ec2.NewFromConfig(cfg)

	// Temporary security group in the default VPC; no ingress rules. The
	// instance needs no connectivity for a provider-effect receipt.
	vpcs, err := client.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{
		Filters: []ec2types.Filter{{Name: aws.String("isDefault"), Values: []string{"true"}}},
	})
	if err != nil || len(vpcs.Vpcs) == 0 {
		t.Fatalf("default vpc: %v", err)
	}
	vpcID := aws.ToString(vpcs.Vpcs[0].VpcId)
	sgName := fmt.Sprintf("sufuree-qual-ec2-%d", time.Now().UnixNano())
	sg, err := client.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
		GroupName:   aws.String(sgName),
		Description: aws.String("sufuree live EC2 qualification (temporary)"),
		VpcId:       aws.String(vpcID),
	})
	if err != nil {
		t.Fatal(err)
	}
	sgID := aws.ToString(sg.GroupId)

	ex, err := ec2exec.New(ec2exec.Config{
		Client:        client,
		DeploymentUID: "qual-ec2",
		AllowEffects:  true,
		FenceTimeout:  3 * time.Minute,
		PollInterval:  3 * time.Second,
		Launch: ec2exec.Launch{
			AMI: ami, InstanceType: instType, SubnetID: subnet,
			SecurityGroupID: sgID, NamePrefix: "sufuree-qual",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	gated := &executor.Gated{
		Inner: ex,
		Ledger: &budget.Ledger{Grant: budget.Grant{
			DeploymentUID: "qual-ec2", MaxHosts: 1, MaxMemoryMiB: 512,
			Expires: time.Now().UTC().Add(10 * time.Minute),
		}},
	}

	var instanceID string
	// Safety-net cleanup: terminate the instance and delete the SG even if the
	// test fails partway. Terminating an already-terminated instance is a no-op.
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer ccancel()
		if instanceID != "" {
			_ = ex.Fence(cctx, instanceID)
		}
		for i := 0; i < 10; i++ {
			_, derr := client.DeleteSecurityGroup(cctx, &ec2.DeleteSecurityGroupInput{GroupId: aws.String(sgID)})
			if derr == nil {
				return
			}
			time.Sleep(3 * time.Second)
		}
		t.Logf("warning: could not delete security group %s (no cost; remove manually)", sgID)
	})

	opID := fmt.Sprintf("qual-ec2-create-%d", time.Now().UnixNano())
	t0 := time.Now()
	ref, err := gated.CreateHost(ctx, executor.CreateHostInput{
		OperationID: opID, Shape: instType, MemoryMiB: 512, CPU: 2,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	instanceID = ref.ProviderID
	createMS := time.Since(t0).Milliseconds()
	t.Logf("created %s in %dms", instanceID, createMS)

	d, err := gated.Describe(ctx, instanceID)
	if err != nil {
		t.Fatalf("describe after create: %v", err)
	}
	if d.State != "pending" && d.State != "running" {
		t.Fatalf("unexpected state after create: %q", d.State)
	}

	// ClientToken idempotency: the same operation ID must resolve to the same
	// instance, not a second purchase.
	ref2, err := gated.CreateHost(ctx, executor.CreateHostInput{
		OperationID: opID, Shape: instType, MemoryMiB: 512, CPU: 2,
	})
	if err != nil {
		t.Fatalf("idempotent re-create: %v", err)
	}
	if ref2.ProviderID != instanceID {
		t.Fatalf("idempotency broken: %s != %s", ref2.ProviderID, instanceID)
	}
	if hosts, _ := gated.Ledger.Usage(); hosts != 1 {
		t.Fatalf("idempotent re-create double-counted: %d hosts", hosts)
	}

	// Fence: terminate and wait for observed terminal state.
	t1 := time.Now()
	if err := gated.TerminateHost(ctx, executor.TerminateHostInput{
		OperationID: opID, ProviderID: instanceID,
	}); err != nil {
		t.Fatalf("terminate/fence: %v", err)
	}
	fenceMS := time.Since(t1).Milliseconds()
	d, err = gated.Describe(ctx, instanceID)
	if err != nil && !errors.Is(err, executor.ErrNotFound) {
		t.Fatalf("describe after fence: %v", err)
	}
	if err == nil && d.State != "terminated" {
		t.Fatalf("not terminated after fence: %q", d.State)
	}
	if hosts, _ := gated.Ledger.Usage(); hosts != 0 {
		t.Fatalf("grant usage not released after fence: %d hosts", hosts)
	}
	t.Logf("fenced %s to terminal in %dms", instanceID, fenceMS)

	receipt := map[string]any{
		"scenario":      "live-ec2-create-terminate-fence",
		"aws_spend":     true,
		"region":        region,
		"ami":           ami,
		"instance_type": instType,
		"instance_id":   instanceID,
		"create_ms":     createMS,
		"fence_ms":      fenceMS,
		"idempotent":    true,
		"qualified_at":  time.Now().UTC().Format(time.RFC3339),
	}
	if dir := os.Getenv("SUFUREE_QUAL_RECEIPT_DIR"); dir != "" {
		buf, err := json.MarshalIndent(receipt, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "live-ec2-create-terminate-fence.json"), append(buf, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
