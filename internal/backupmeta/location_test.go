package backupmeta

import "testing"

func TestDisplayLocation(t *testing.T) {
	for _, tc := range []struct {
		storage, bucket, output, want string
		encrypted, valid              bool
	}{
		{storage: "S3", bucket: "bucket", output: "prod_backups/db dumps/seed", want: "prod_backups/db dumps/seed", valid: true},
		{storage: "S3", bucket: "bucket", output: "prod_backups/db dumps/seed", encrypted: true, want: "s3://bucket/prod_backups/db dumps/seed/", valid: true},
		{storage: "S3", bucket: "bucket", output: "prod_backups/seed/", encrypted: true, want: "s3://bucket/prod_backups/seed/", valid: true},
		{storage: "PVC", output: "/backups/db dumps/prod_backups/seed", want: "/backups/db dumps/prod_backups/seed", valid: true},
		{storage: "PVC", output: "/backups/prod_backups/seed", encrypted: true, want: "/backups/prod_backups/seed", valid: true},
		{storage: "S3", output: "prefix", encrypted: true},
		{storage: "S3", bucket: "bucket"},
		{storage: "unknown", output: "prefix"},
	} {
		t.Run(tc.storage+tc.output, func(t *testing.T) {
			got, err := DisplayLocation(tc.storage, tc.bucket, tc.output, tc.encrypted)
			if (err == nil) != tc.valid || got != tc.want {
				t.Fatalf("got %q/%v, want %q valid=%v", got, err, tc.want, tc.valid)
			}
		})
	}
}
