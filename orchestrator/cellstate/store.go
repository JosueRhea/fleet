package cellstate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Bucket keys. Each host pulls its own doc; every edge in the cell pulls the same edge doc.
func HostDocKey(host string) string    { return "docs/hosts/" + host + ".json" }
func HostStatusKey(host string) string { return "status/hosts/" + host + ".json" }

const (
	EdgeDocKey       = "docs/edge.json"
	HostStatusPrefix = "status/hosts/"
	EdgeStatusPrefix = "status/edges/"
)

type Store struct {
	c      *minio.Client
	bucket string
}

// OpenStore connects using CCLOUD_S3_ENDPOINT (e.g. http://127.0.0.1:9100), CCLOUD_BUCKET,
// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
func OpenStore(ctx context.Context) (*Store, error) {
	u, err := url.Parse(os.Getenv("CCLOUD_S3_ENDPOINT"))
	if err != nil || u.Host == "" {
		return nil, errors.New("CCLOUD_S3_ENDPOINT must be a URL like http://host:port")
	}
	bucket := os.Getenv("CCLOUD_BUCKET")
	if bucket == "" {
		return nil, errors.New("CCLOUD_BUCKET is not set")
	}
	c, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewEnvAWS(),
		Secure: u.Scheme == "https",
		Region: "us-east-1",
	})
	if err != nil {
		return nil, err
	}
	ok, err := c.BucketExists(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if !ok {
		if err := c.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
			return nil, err
		}
	}
	return &Store{c: c, bucket: bucket}, nil
}

// Get returns nil, nil if the key does not exist.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	o, err := s.c.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer o.Close()
	b, err := io.ReadAll(o)
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return nil, nil
	}
	return b, err
}

func (s *Store) Put(ctx context.Context, key string, b []byte) error {
	_, err := s.c.PutObject(ctx, s.bucket, key, bytes.NewReader(b), int64(len(b)),
		minio.PutObjectOptions{ContentType: "application/json"})
	return err
}

// List returns the contents of every object under prefix, keyed by object name.
func (s *Store) List(ctx context.Context, prefix string) (map[string][]byte, error) {
	out := map[string][]byte{}
	for o := range s.c.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if o.Err != nil {
			return nil, o.Err
		}
		b, err := s.Get(ctx, o.Key)
		if err != nil {
			return nil, err
		}
		if b != nil {
			out[o.Key] = b
		}
	}
	return out, nil
}

// OpenCell is the common setup for the worker and CLI: state bucket from env, signing key from path.
func OpenCell(ctx context.Context, keyPath string) (*Store, *Signer, error) {
	st, err := OpenStore(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("state bucket: %w", err)
	}
	sg, err := LoadSigner(keyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("signing key: %w", err)
	}
	return st, sg, nil
}
