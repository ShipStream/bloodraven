package rustfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type Credentials struct {
	AccessKey string
	SecretKey string
	Region    string
}

// EnsureBucket creates bucket when it is missing. Requests are signed with
// AWS Signature V4 so the runner does not depend on a separate aws-cli image
// inside the cluster.
func EnsureBucket(ctx context.Context, endpoint, bucket string, creds Credentials) error {
	if bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	client, err := newS3Client(endpoint, creds)
	if err != nil {
		return err
	}

	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err == nil {
		return nil
	} else {
		var respErr *smithyhttp.ResponseError
		if !errors.As(err, &respErr) || respErr.HTTPStatusCode() != http.StatusNotFound {
			return fmt.Errorf("head bucket %q: %w", bucket, err)
		}
	}

	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err == nil {
		return nil
	} else {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
				return nil
			}
		}
		return fmt.Errorf("create bucket %q: %w", bucket, err)
	}
}

// GetObject fetches an object from the RustFS bucket. Returns found=false
// (with nil error) when the key does not exist so callers can distinguish a
// missing object from a read error. Signed with AWS SigV4 like EnsureBucket
// so no aws-cli sidecar image is needed. The PITR scenarios use it to read a
// dump's @.json and @.done.json (its gtidExecuted baseline and end time)
// and the per-site binlog manifests straight from storage.
func GetObject(ctx context.Context, endpoint, bucket, key string, creds Credentials) (data []byte, found bool, err error) {
	if bucket == "" || key == "" {
		return nil, false, fmt.Errorf("bucket and key are required")
	}
	client, err := newS3Client(endpoint, creds)
	if err != nil {
		return nil, false, err
	}

	out, gerr := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if gerr != nil {
		var respErr *smithyhttp.ResponseError
		if errors.As(gerr, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound {
			return nil, false, nil
		}
		var apiErr smithy.APIError
		if errors.As(gerr, &apiErr) && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound") {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get object %q/%q: %w", bucket, key, gerr)
	}
	defer out.Body.Close()
	body, rerr := io.ReadAll(out.Body)
	if rerr != nil {
		return nil, true, fmt.Errorf("read object %q/%q body: %w", bucket, key, rerr)
	}
	return body, true, nil
}

// ListKeys returns every key under prefix. It pages through ListObjectsV2
// with a small page size, so a dump spans several pages and the listing
// exercises pagination against the real store.
func ListKeys(ctx context.Context, endpoint, bucket, prefix string, creds Credentials) ([]string, error) {
	if bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	client, err := newS3Client(endpoint, creds)
	if err != nil {
		return nil, err
	}
	var keys []string
	pages := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(listPageSize),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list %q/%q: %w", bucket, prefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
	}
	return keys, nil
}

// DeletePrefix deletes every object under prefix, one DeleteObject per key,
// and returns how many it deleted. prefix must be non-empty and end in "/",
// so a scenario cannot delete a whole bucket or a sibling prefix sharing a
// name stem.
func DeletePrefix(ctx context.Context, endpoint, bucket, prefix string, creds Credentials) (int, error) {
	if bucket == "" || prefix == "" || !strings.HasSuffix(prefix, "/") || strings.Trim(prefix, "/") == "" {
		return 0, fmt.Errorf("bucket and a non-root prefix ending in / are required (got %q)", prefix)
	}
	keys, err := ListKeys(ctx, endpoint, bucket, prefix, creds)
	if err != nil {
		return 0, err
	}
	client, err := newS3Client(endpoint, creds)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, k := range keys {
		if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(k)}); err != nil {
			return deleted, fmt.Errorf("delete %q/%q: %w", bucket, k, err)
		}
		deleted++
	}
	return deleted, nil
}

// listPageSize keeps ListKeys pages small enough that a backup's objects
// span several of them.
const listPageSize = 5

func newS3Client(endpoint string, creds Credentials) (*s3.Client, error) {
	if creds.AccessKey == "" || creds.SecretKey == "" {
		return nil, fmt.Errorf("access key and secret key are required")
	}
	if creds.Region == "" {
		creds.Region = "us-east-1"
	}
	endpoint = strings.TrimRight(endpoint, "/")
	if _, err := url.Parse(endpoint); err != nil {
		return nil, fmt.Errorf("parse endpoint: %w", err)
	}
	return s3.NewFromConfig(aws.Config{
		Region:      creds.Region,
		Credentials: credentials.NewStaticCredentialsProvider(creds.AccessKey, creds.SecretKey, ""),
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	}), nil
}
