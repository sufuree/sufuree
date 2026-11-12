package ec2live

import "testing"

func TestLoadDefaultPin(t *testing.T) {
	path := DefaultPinPath()
	if path == "" {
		t.Skip("repo pin not found from cwd")
	}
	p, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if p.Region != "us-west-1" || p.GrantMaxHosts != 1 {
		t.Fatalf("pin = %+v", p)
	}
}

func TestValidRejectsHigherHostGrant(t *testing.T) {
	p := Pin{
		Schema: Schema, Region: "us-west-1", AMI: "ami-x", InstanceType: "t3.micro",
		SubnetID: "subnet-x", SecurityGroupID: "sg-x", MemoryMiB: 1024,
		GrantMaxHosts: 2, GrantMaxMemoryMiB: 2048,
	}
	if err := p.Valid(); err == nil {
		t.Fatal("want grant_max_hosts error")
	}
}

func TestEnabled(t *testing.T) {
	t.Setenv(EnvEnable, "")
	if Enabled() {
		t.Fatal("empty")
	}
	t.Setenv(EnvEnable, "1")
	if !Enabled() {
		t.Fatal("1")
	}
}
