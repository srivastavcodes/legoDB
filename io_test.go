package bpTree

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// todo: write disk manager tests as well

func TestMemIO_ReadWrite(t *testing.T) {
	buf := make([]byte, 0)
	buf = bytes.Repeat([]byte("write"), 1000)

	mf := NewMemFile()
	n, err := mf.WriteAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)

	buf = make([]byte, n)
	n, err = mf.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
}

func TestMemIO_ReadPastEnd(t *testing.T) {
	buf := make([]byte, 0)
	buf = bytes.Repeat([]byte("write"), 1000)

	mf := NewMemFile()
	n, err := mf.WriteAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)

	buf = make([]byte, n)
	n, err = mf.ReadAt(buf, int64(n))
	require.EqualError(t, err, "EOF")
	require.Equal(t, n, 0)

	buf = make([]byte, 5001)
	n, err = mf.ReadAt(buf, 0)
	require.EqualError(t, err, "EOF")
	require.Less(t, n, len(buf))
	require.Equal(t, n, len(mf.buf))
}

func TestMemIO_TruncateSync(t *testing.T) {
	buf := make([]byte, 0)
	buf = bytes.Repeat([]byte("write"), 1000)

	mf := NewMemFile()

	n, err := mf.WriteAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)

	buf = make([]byte, n)
	n, err = mf.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)

	size := int64(n - 500)
	err = mf.Truncate(size)
	require.NoError(t, err)
	require.Equal(t, size, mf.Size())
}

func TestFileIO_ReadWrite(t *testing.T) {
	buf := make([]byte, 0)
	buf = bytes.Repeat([]byte("write"), 1000)

	fd, err := OpenFile("fileIO-test")
	require.NoError(t, err)
	defer func() { _ = os.Remove(fd.fd.Name()) }()

	n, err := fd.WriteAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)

	buf = make([]byte, n)
	n, err = fd.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
}

func TestFileIO_ReadPastEnd(t *testing.T) {
	buf := make([]byte, 0)
	buf = bytes.Repeat([]byte("write"), 1000)

	fd, err := OpenFile("fileIO-test")
	require.NoError(t, err)
	defer func() { _ = os.Remove(fd.fd.Name()) }()

	n, err := fd.WriteAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)

	buf = make([]byte, n)
	n, err = fd.ReadAt(buf, int64(n))
	require.EqualError(t, err, "EOF")
	require.Equal(t, n, 0)

	buf = make([]byte, 5001)
	n, err = fd.ReadAt(buf, 0)
	require.EqualError(t, err, "EOF")
	require.Less(t, n, len(buf))

	require.Equal(t, int64(n), fd.Size())
}

func TestFileIO_TruncateSync(t *testing.T) {
	buf := make([]byte, 0)
	buf = bytes.Repeat([]byte("write"), 1000)

	fd, err := OpenFile("fileIO-test")
	require.NoError(t, err)
	defer func() { _ = os.Remove(fd.fd.Name()) }()

	n, err := fd.WriteAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)

	buf = make([]byte, n)
	n, err = fd.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)

	err = fd.Sync()
	require.NoError(t, err)

	size := int64(n - 500)
	err = fd.Truncate(size)
	require.NoError(t, err)
	require.Equal(t, size, fd.Size())
}
