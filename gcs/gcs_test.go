package gcs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iterator"

	"github.com/apiobuild/blobkv/config"
	"github.com/apiobuild/blobkv/gcs"
	"github.com/apiobuild/blobkv/model"
)

var (
	json = fmt.Sprintf(`{
		"foo": "bar"
	}`)
	mockRC = io.NopCloser(bytes.NewReader([]byte(json)))
)

type clientMock struct {
	stiface.Client
}

type bucketMock1 struct {
	stiface.BucketHandle
}

type existObjectMock struct {
	stiface.ObjectHandle
}

type mockWriter struct {
	stiface.Writer
}

type mockReader struct {
	stiface.Reader
	rc io.ReadCloser
}

func (m bucketMock1) Object(name string) stiface.ObjectHandle {
	return existObjectMock{}
}

type objectItMock struct {
	stiface.ObjectIterator
	i    int
	next []storage.ObjectAttrs
}

func (it *objectItMock) Next() (a *storage.ObjectAttrs, err error) {
	if it.i == len(it.next) {
		err = iterator.Done
		return
	}

	a = &it.next[it.i]
	it.i++
	return
}

func (m bucketMock1) Objects(ctx context.Context, q *storage.Query) (it stiface.ObjectIterator) {
	it = &objectItMock{
		i: 0,
		next: []storage.ObjectAttrs{
			{Prefix: Key},
			{Prefix: Key},
		},
	}
	return
}

func (m existObjectMock) Attrs(_ context.Context) (attrs *storage.ObjectAttrs, err error) {
	attrs = &storage.ObjectAttrs{}
	return
}

func (m existObjectMock) NewWriter(_ context.Context) (w stiface.Writer) {
	w = &mockWriter{}
	return
}

func (m existObjectMock) NewReader(_ context.Context) (r stiface.Reader, err error) {
	r = &mockReader{
		rc: mockRC,
	}
	return
}

func (m existObjectMock) Delete(_ context.Context) (err error) {
	return
}

func (m *mockReader) Read(p []byte) (int, error) {
	return m.rc.Read(p)
}

func (m *mockReader) Close() error {
	return m.rc.Close()
}

type bucketMock2 struct {
	stiface.BucketHandle
}

func (m bucketMock2) Object(name string) stiface.ObjectHandle {
	return newObjectMock{}
}

type newObjectMock struct {
	stiface.ObjectHandle
}

func (m newObjectMock) Attrs(ctx context.Context) (attrs *storage.ObjectAttrs, err error) {
	attrs = &storage.ObjectAttrs{}
	err = errors.New("storage: object doesn't exist")
	return
}

func (m newObjectMock) NewWriter(ctx context.Context) (w stiface.Writer) {
	w = &mockWriter{}
	return
}

func (m *mockWriter) Attrs() (attrs *storage.ObjectAttrs) {
	return &storage.ObjectAttrs{}
}

func (m *mockWriter) Close() error {
	return nil
}
func (m *mockWriter) Write(p []byte) (n int, err error) {
	return 1, nil
}

func (m mockWriter) ObjectAttrs() *storage.ObjectAttrs {
	return &storage.ObjectAttrs{}
}

func getMockI(bucket stiface.BucketHandle) gcs.I {
	return gcs.I{
		Client: clientMock{},
		Config: config.Storage{
			Prefix: "stacks",
		},
		Bucket: bucket,
	}
}

var (
	Key      = "key"
	Payload  = model.Payload{"foo": "bar"}
	Metadata = model.Metadata{"foo": "bar"}
)

func TestAdd(t *testing.T) {
	tests := []struct {
		Bucket   stiface.BucketHandle
		Metadata *model.Metadata
	}{
		{
			// exist object
			Bucket:   bucketMock1{},
			Metadata: nil,
		},
		{
			// new object
			Bucket:   bucketMock2{},
			Metadata: nil,
		},
		{
			// new object
			Bucket:   bucketMock2{},
			Metadata: &Metadata,
		},
	}

	for _, tt := range tests {
		cli := getMockI(tt.Bucket)
		err := cli.Add(Key, Payload, tt.Metadata)
		assert.NoError(t, err)
	}
}

func TestForceAdd(t *testing.T) {
	tests := []struct {
		Bucket stiface.BucketHandle
	}{
		{
			// exist object
			Bucket: bucketMock1{},
		},
	}

	for _, tt := range tests {
		cli := getMockI(tt.Bucket)
		err := cli.ForceAdd(Key, Payload, nil)
		assert.NoError(t, err)
	}
}

func TestGet(t *testing.T) {
	tests := []struct {
		Bucket   stiface.BucketHandle
		HasError bool
		Resp     model.Payload
	}{
		{
			// exist object
			Bucket: bucketMock1{},
			Resp:   Payload,
		},
		{
			// new object
			Bucket:   bucketMock2{},
			HasError: true,
		},
	}

	for _, tt := range tests {
		cli := getMockI(tt.Bucket)
		resp, err := cli.Get(Key)

		if tt.HasError {
			assert.EqualError(t, err, "code=404, message=Object Does Not Exists")
		} else {
			assert.NoError(t, err)
			assert.Equal(t, resp, tt.Resp)
		}
	}
}

func TestList(t *testing.T) {
	cli := getMockI(bucketMock1{})
	resp, err := cli.List("prefix")
	assert.NoError(t, err)
	assert.Equal(t, model.Keys{
		model.Key{Key: Key},
		model.Key{Key: Key},
	}, resp)
}

func TestDelete(t *testing.T) {
	tests := []struct {
		Bucket   stiface.BucketHandle
		HasError bool
		Resp     model.Payload
	}{
		{
			// exist object
			Bucket: bucketMock1{},
			Resp:   Payload,
		},
		{
			// new object
			Bucket:   bucketMock2{},
			HasError: true,
		},
	}

	for _, tt := range tests {
		cli := getMockI(tt.Bucket)
		err := cli.Delete(Key)

		if tt.HasError {
			assert.EqualError(t, err, "code=404, message=Object Does Not Exists")
		} else {
			assert.NoError(t, err)
		}
	}
}
