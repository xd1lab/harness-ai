package blobtest_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/platform/blob"
	"github.com/xd1lab/harness-ai/internal/platform/blob/blobtest"
)

var ctx = context.Background()

func ref(tenant, key string) blob.Ref { return blob.Ref{TenantID: tenant, Key: key} }

func TestFakeBlobStore_PutGetStat(t *testing.T) {
	bs := blobtest.NewFakeBlobStore()
	r := ref("tenant-A", "key1")
	data := []byte("hello blob")

	obj, err := bs.Put(ctx, r, "text/plain", bytes.NewReader(data))
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), obj.SizeBytes)
	assert.Equal(t, "text/plain", obj.MediaType)

	gotObj, rc, err := bs.Get(ctx, r)
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()
	gotData, _ := io.ReadAll(rc)
	assert.Equal(t, data, gotData)
	assert.Equal(t, obj.SizeBytes, gotObj.SizeBytes)

	statObj, err := bs.Stat(ctx, r)
	require.NoError(t, err)
	assert.Equal(t, obj.SizeBytes, statObj.SizeBytes)
}

func TestFakeBlobStore_NotFound(t *testing.T) {
	bs := blobtest.NewFakeBlobStore()
	_, _, err := bs.Get(ctx, ref("t", "missing"))
	assert.ErrorIs(t, err, blob.ErrNotFound)

	_, err = bs.Stat(ctx, ref("t", "missing"))
	assert.ErrorIs(t, err, blob.ErrNotFound)
}

func TestFakeBlobStore_TenantIsolation(t *testing.T) {
	bs := blobtest.NewFakeBlobStore()
	// Put under tenant-A.
	_, err := bs.Put(ctx, ref("tenant-A", "same-key"), "text/plain", bytes.NewReader([]byte("A data")))
	require.NoError(t, err)
	// tenant-B with the same key must not see it.
	_, _, err = bs.Get(ctx, ref("tenant-B", "same-key"))
	assert.ErrorIs(t, err, blob.ErrNotFound)
}

func TestFakeBlobStore_TooLarge(t *testing.T) {
	bs := blobtest.NewFakeBlobStore()
	bs.MaxSize = 4
	_, err := bs.Put(ctx, ref("t", "k"), "text/plain", bytes.NewReader([]byte("toolarge")))
	assert.ErrorIs(t, err, blob.ErrTooLarge)
}

func TestFakeBlobStore_IdempotentPut(t *testing.T) {
	bs := blobtest.NewFakeBlobStore()
	r := ref("t", "k")
	_, err := bs.Put(ctx, r, "text/plain", bytes.NewReader([]byte("first")))
	require.NoError(t, err)
	_, err = bs.Put(ctx, r, "text/plain", bytes.NewReader([]byte("first")))
	require.NoError(t, err)
}

func TestFakeBlobStore_DeleteIdempotent(t *testing.T) {
	bs := blobtest.NewFakeBlobStore()
	r := ref("t", "k")
	_, _ = bs.Put(ctx, r, "text/plain", bytes.NewReader([]byte("x")))
	assert.NoError(t, bs.Delete(ctx, r))
	assert.NoError(t, bs.Delete(ctx, r)) // deleting absent is ok
	_, _, err := bs.Get(ctx, r)
	assert.ErrorIs(t, err, blob.ErrNotFound)
}
