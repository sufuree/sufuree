// Command sufuree-infra declares Sufuree's AWS infrastructure: the KMS keys
// that wrap the controller CA key and the replica credential provisioning
// key.
package main

import (
	"encoding/json"

	"github.com/pulumi/pulumi-aws/sdk/v7/go/aws"
	"github.com/pulumi/pulumi-aws/sdk/v7/go/aws/kms"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
)

// keyPolicy lets the account administer a key but not use it, and the
// listed principals use it only for one purpose: every encrypt and decrypt
// must carry that purpose in its encryption context. Grants are denied to
// everyone, so an IAM administrator can't use one to decrypt without the
// context. PutKeyPolicy stays with the account as the audited break-glass
// (it also prevents lockout).
func keyPolicy(accountID, purpose string, uses ...map[string]any) (string, error) {
	root := "arn:aws:iam::" + accountID + ":root"
	statements := []map[string]any{
		{
			"Sid":       "AccountAdministersNotUses",
			"Effect":    "Allow",
			"Principal": map[string]any{"AWS": root},
			"Action": []string{
				"kms:CreateAlias", "kms:Describe*", "kms:Enable*", "kms:List*", "kms:Put*",
				"kms:Update*", "kms:Revoke*", "kms:Disable*", "kms:Get*", "kms:Delete*",
				"kms:TagResource", "kms:UntagResource", "kms:ScheduleKeyDeletion", "kms:CancelKeyDeletion",
				"kms:RotateKeyOnDemand",
			},
			"Resource": "*",
		},
		{
			"Sid":       "NoGrants",
			"Effect":    "Deny",
			"Principal": map[string]any{"AWS": "*"},
			"Action":    []string{"kms:CreateGrant"},
			"Resource":  "*",
		},
	}
	for _, u := range uses {
		u["Effect"] = "Allow"
		u["Resource"] = "*"
		u["Condition"] = map[string]any{
			"StringEquals": map[string]any{"kms:EncryptionContext:purpose": purpose},
		}
		statements = append(statements, u)
	}
	b, err := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": statements})
	return string(b), err
}

// caKeyPolicy: the operator wraps and unwraps the Sufuree CA key.
func caKeyPolicy(accountID, operatorArn string) (string, error) {
	return keyPolicy(accountID, "sufuree-control-ca", map[string]any{
		"Sid":       "OperatorWrapsSufureeCA",
		"Principal": map[string]any{"AWS": operatorArn},
		"Action":    []string{"kms:Encrypt", "kms:Decrypt"},
	})
}

// replicaKeyPolicy: the operator creates the replica credential
// provisioning key (and may run a controller); controllers only unwrap it
// at start.
func replicaKeyPolicy(accountID, operatorArn string, controllerArns []string) (string, error) {
	uses := []map[string]any{{
		"Sid":       "OperatorWrapsReplicaProvisioningKey",
		"Principal": map[string]any{"AWS": operatorArn},
		"Action":    []string{"kms:Encrypt", "kms:Decrypt"},
	}}
	if len(controllerArns) > 0 {
		uses = append(uses, map[string]any{
			"Sid":       "ControllersUnwrapReplicaProvisioningKey",
			"Principal": map[string]any{"AWS": controllerArns},
			"Action":    []string{"kms:Decrypt"},
		})
	}
	return keyPolicy(accountID, "sufuree-replica-credentials", uses...)
}

// wrappingKey declares a rotating symmetric KMS key with policy and alias.
func wrappingKey(ctx *pulumi.Context, name, description, policy string) (*kms.Key, error) {
	key, err := kms.NewKey(ctx, name, &kms.KeyArgs{
		Description:           pulumi.String(description),
		KeyUsage:              pulumi.String("ENCRYPT_DECRYPT"),
		CustomerMasterKeySpec: pulumi.String("SYMMETRIC_DEFAULT"),
		EnableKeyRotation:     pulumi.Bool(true),
		DeletionWindowInDays:  pulumi.Int(30),
		Policy:                pulumi.String(policy),
		Tags:                  pulumi.StringMap{"project": pulumi.String("sufuree"), "data": pulumi.String("phi-adjacent")},
	})
	if err != nil {
		return nil, err
	}
	if _, err := kms.NewAlias(ctx, name, &kms.AliasArgs{
		Name:        pulumi.String("alias/" + name),
		TargetKeyId: key.KeyId,
	}); err != nil {
		return nil, err
	}
	return key, nil
}

func main() {
	pulumi.Run(func(ctx *pulumi.Context) error {
		cfg := config.New(ctx, "")
		operatorArn := cfg.Require("operatorArn")
		// controllerRoleArns: IAM roles of controllers that unwrap the
		// replica provisioning key at start (optional).
		var controllerArns []string
		_ = cfg.TryObject("controllerRoleArns", &controllerArns)
		id, err := aws.GetCallerIdentity(ctx, nil)
		if err != nil {
			return err
		}
		policy, err := caKeyPolicy(id.AccountId, operatorArn)
		if err != nil {
			return err
		}
		key, err := wrappingKey(ctx, "sufuree-ca", "Sufuree controller CA key wrapping (RFC-0001 envelope custody)", policy)
		if err != nil {
			return err
		}
		ctx.Export("caKeyArn", key.Arn)
		ctx.Export("caKeyAlias", pulumi.String("alias/sufuree-ca"))

		rpolicy, err := replicaKeyPolicy(id.AccountId, operatorArn, controllerArns)
		if err != nil {
			return err
		}
		rkey, err := wrappingKey(ctx, "sufuree-replica-credentials", "Sufuree replica credential provisioning key wrapping", rpolicy)
		if err != nil {
			return err
		}
		ctx.Export("replicaKeyArn", rkey.Arn)
		ctx.Export("replicaKeyAlias", pulumi.String("alias/sufuree-replica-credentials"))
		return nil
	})
}
