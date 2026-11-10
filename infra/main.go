// Command sufuree-infra declares Sufuree's AWS infrastructure: the KMS key
// that wraps the controller CA key.
package main

import (
	"encoding/json"

	"github.com/pulumi/pulumi-aws/sdk/v7/go/aws"
	"github.com/pulumi/pulumi-aws/sdk/v7/go/aws/kms"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
)

// caKeyPolicy lets the account administer the key (never use it) and the
// operator use it only for Sufuree CA wrapping: every encrypt and decrypt
// must carry the CA purpose in its encryption context.
func caKeyPolicy(accountID, operatorArn string) (string, error) {
	root := "arn:aws:iam::" + accountID + ":root"
	doc := map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{
			{
				"Sid":       "AccountAdministersNotUses",
				"Effect":    "Allow",
				"Principal": map[string]any{"AWS": root},
				"Action": []string{
					"kms:Create*", "kms:Describe*", "kms:Enable*", "kms:List*", "kms:Put*",
					"kms:Update*", "kms:Revoke*", "kms:Disable*", "kms:Get*", "kms:Delete*",
					"kms:TagResource", "kms:UntagResource", "kms:ScheduleKeyDeletion", "kms:CancelKeyDeletion",
					"kms:RotateKeyOnDemand",
				},
				"Resource": "*",
			},
			{
				"Sid":       "OperatorWrapsSufureeCA",
				"Effect":    "Allow",
				"Principal": map[string]any{"AWS": operatorArn},
				"Action":    []string{"kms:Encrypt", "kms:Decrypt"},
				"Resource":  "*",
				"Condition": map[string]any{
					"StringEquals": map[string]any{"kms:EncryptionContext:purpose": "sufuree-control-ca"},
				},
			},
		},
	}
	b, err := json.Marshal(doc)
	return string(b), err
}

func main() {
	pulumi.Run(func(ctx *pulumi.Context) error {
		cfg := config.New(ctx, "")
		operatorArn := cfg.Require("operatorArn")
		id, err := aws.GetCallerIdentity(ctx, nil)
		if err != nil {
			return err
		}
		policy, err := caKeyPolicy(id.AccountId, operatorArn)
		if err != nil {
			return err
		}
		key, err := kms.NewKey(ctx, "sufuree-ca", &kms.KeyArgs{
			Description:           pulumi.String("Sufuree controller CA key wrapping (RFC-0001 envelope custody)"),
			KeyUsage:              pulumi.String("ENCRYPT_DECRYPT"),
			CustomerMasterKeySpec: pulumi.String("SYMMETRIC_DEFAULT"),
			EnableKeyRotation:     pulumi.Bool(true),
			DeletionWindowInDays:  pulumi.Int(30),
			Policy:                pulumi.String(policy),
			Tags:                  pulumi.StringMap{"project": pulumi.String("sufuree"), "data": pulumi.String("phi-adjacent")},
		})
		if err != nil {
			return err
		}
		if _, err := kms.NewAlias(ctx, "sufuree-ca", &kms.AliasArgs{
			Name:        pulumi.String("alias/sufuree-ca"),
			TargetKeyId: key.KeyId,
		}); err != nil {
			return err
		}
		ctx.Export("caKeyArn", key.Arn)
		ctx.Export("caKeyAlias", pulumi.String("alias/sufuree-ca"))
		return nil
	})
}
