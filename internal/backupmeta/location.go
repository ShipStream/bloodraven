package backupmeta

import (
	"fmt"
	"strings"
)

// DisplayLocation preserves the destination convention used by each producer.
func DisplayLocation(storageType, bucket, output string, encrypted bool) (string, error) {
	if output == "" {
		return "", fmt.Errorf("backup Job has no output destination")
	}
	switch storageType {
	case "PVC":
		return output, nil
	case "S3":
		if !encrypted {
			return output, nil
		}
		if bucket == "" {
			return "", fmt.Errorf("encrypted S3 backup Job has no bucket")
		}
		return fmt.Sprintf("s3://%s/%s/", bucket, strings.TrimSuffix(output, "/")), nil
	default:
		return "", fmt.Errorf("backup Job has unknown storage type %q", storageType)
	}
}
