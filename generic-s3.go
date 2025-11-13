package cmgs3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/caddyserver/certmagic"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var _ certmagic.Storage = &S3Storage{}

type S3Opts struct {
	Endpoint        string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string

	ObjPrefix string
	Insecure  bool

	// EncryptionKey is optional. If you do not wish to encrypt your certficates and key inside the S3 bucket, leave it empty.
	EncryptionKey []byte
}

type S3Storage struct {
	prefix   string
	bucket   string
	s3client *minio.Client

	iowrap IO
}

func NewS3Storage(ctx context.Context, opts S3Opts) (*S3Storage, error) {
	gs3 := &S3Storage{
		prefix: opts.ObjPrefix,
		bucket: opts.Bucket,
	}

	if opts.EncryptionKey == nil || len(opts.EncryptionKey) == 0 {
		log.Println("Clear text certificate storage active")
		gs3.iowrap = &CleartextIO{}
	} else if len(opts.EncryptionKey) != 32 {
		return nil, errors.New("encryption key must have exactly 32 bytes")
	} else {
		log.Println("Encrypted certificate storage active")
		sb := &SecretBoxIO{}
		copy(sb.SecretKey[:], opts.EncryptionKey)
		gs3.iowrap = sb
	}

	var err error
	gs3.s3client, err = minio.New(opts.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(opts.AccessKeyID, opts.SecretAccessKey, ""),
		Secure: !opts.Insecure,
	})
	if err != nil {
		return nil, fmt.Errorf("geting s3 client: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ok, err := gs3.s3client.BucketExists(ctx, opts.Bucket)
	if err != nil {
		return nil, fmt.Errorf("checking if bucket exists: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("S3 bucket %s does not exist", opts.Bucket)
	}

	return gs3, nil
}

var (
	LockExpiration   = 2 * time.Minute
	LockPollInterval = 1 * time.Second
	LockTimeout      = 15 * time.Second
)

func (gs *S3Storage) Lock(ctx context.Context, key string) error {
	var respErr minio.ErrorResponse
	startedAt := time.Now()
	lockFile := gs.objLockName(key)

	for {
		obj, err := gs.s3client.GetObject(ctx, gs.bucket, lockFile, minio.GetObjectOptions{})
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			return gs.putLockFile(ctx, key)
		}

		if err != nil {
			return fmt.Errorf("acquiring lock failed: %w", err)
		}

		buf, err := ioutil.ReadAll(obj)
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			return gs.putLockFile(ctx, key)
		}

		if err != nil {
			// Retry
			return fmt.Errorf("reading lock file: %w", err)
		}

		lockedAt, err := time.Parse(time.RFC3339, string(buf))
		if err != nil {
			// Lock file does not make sense, overwrite.
			return gs.putLockFile(ctx, key)
		}

		if startedAt.Add(LockTimeout).After(lockedAt) {
			// Existing lock file expired, overwrite.
			return gs.putLockFile(ctx, key)
		}

		// Has been locked for too long. There is a problem
		if startedAt.Add(LockExpiration).After(lockedAt) {
			log.Printf(
				"[INFO][S3Storage] Lock for '%s' is stale (locked at: %s); removing then retrying: %s",
				key, lockedAt, gs.objLockName(key),
			)
			err = gs.Delete(ctx, lockFile)
			if err != nil {
				if errors.As(err, &respErr) && respErr.StatusCode != http.StatusNotFound {
					return fmt.Errorf("unable to delete stale lockfile; deadlocked: %w", err)
				}
			}

			// Existing lock file is stale. Replace
			return gs.putLockFile(ctx, key)
		}

		// lockfile exists and is not stale;
		// just wait a moment and try again,
		// or return if context cancelled
		select {
		case <-time.After(LockPollInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (gs *S3Storage) putLockFile(ctx context.Context, key string) error {
	// Object does not exist, we're creating a lock file.
	r := bytes.NewReader([]byte(time.Now().Format(time.RFC3339)))
	_, err := gs.s3client.PutObject(
		ctx,
		gs.bucket,
		gs.objLockName(key),
		r,
		int64(r.Len()),
		minio.PutObjectOptions{},
	)

	return err
}

func (gs *S3Storage) Unlock(ctx context.Context, key string) error {
	return gs.s3client.RemoveObject(ctx, gs.bucket, gs.objLockName(key), minio.RemoveObjectOptions{})
}

func (gs *S3Storage) Store(ctx context.Context, key string, value []byte) error {
	r := gs.iowrap.ByteReader(value)
	_, err := gs.s3client.PutObject(ctx,
		gs.bucket,
		gs.objName(key),
		r,
		int64(r.Len()),
		minio.PutObjectOptions{},
	)

	return err
}

func (gs *S3Storage) Load(ctx context.Context, key string) ([]byte, error) {
	r, err := gs.s3client.GetObject(ctx, gs.bucket, gs.objName(key), minio.GetObjectOptions{})
	if err != nil {
		var respErr minio.ErrorResponse
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			return nil, fs.ErrNotExist
		}
		return nil, err
	}
	defer r.Close()

	buf, err := ioutil.ReadAll(gs.iowrap.WrapReader(r))
	if err != nil {
		var respErr minio.ErrorResponse
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			return nil, fs.ErrNotExist
		}

		return nil, err
	}

	return buf, nil
}

func (gs *S3Storage) Delete(ctx context.Context, key string) error {
	return gs.s3client.RemoveObject(ctx, gs.bucket, gs.objName(key), minio.RemoveObjectOptions{})
}

func (gs *S3Storage) Exists(ctx context.Context, key string) bool {
	_, err := gs.s3client.StatObject(ctx, gs.bucket, gs.objName(key), minio.StatObjectOptions{})
	return err == nil
}

func (gs *S3Storage) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	var keys []string
	for obj := range gs.s3client.ListObjects(ctx, gs.bucket, minio.ListObjectsOptions{
		Prefix:    gs.objName(""),
		Recursive: true,
	}) {
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

func (gs *S3Storage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	var ki certmagic.KeyInfo
	oi, err := gs.s3client.StatObject(ctx, gs.bucket, gs.objName(key), minio.StatObjectOptions{})
	if err != nil {
		var respErr minio.ErrorResponse
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			return ki, fs.ErrNotExist
		}
		return ki, err
	}
	ki.Key = key
	ki.Size = oi.Size
	ki.Modified = oi.LastModified
	ki.IsTerminal = true
	return ki, nil
}

func (gs *S3Storage) objName(key string) string {
	return gs.prefix + "/" + key
}

func (gs *S3Storage) objLockName(key string) string {
	return gs.objName(key) + ".lock"
}
