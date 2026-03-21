package storage

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiskManager_ReadWrite(t *testing.T) {
	pageSizes := []int{4, 8, 16}
	maxPages := 10

	for _, pageSize := range pageSizes {
		t.Run(fmt.Sprintf("pageSize=%d", pageSize), func(t *testing.T) {
			dm := NewDiskManager(NewMemFile(), pageSize)
			for i := uint32(0); i < uint32(maxPages); i++ {
				var (
					want = bytes.Repeat([]byte{byte(i + 1)}, pageSize)
					got  = make([]byte, pageSize)
				)
				err := dm.WritePage(i, want)
				require.NoError(t, err)

				err = dm.ReadPage(i, got)
				require.NoError(t, err)

				require.Equal(t, want, got)
			}
		})
	}
}

func TestDiskManager_UnwrittenPageReturnsZero(t *testing.T) {
	dm := NewDiskManager(NewMemFile(), 8)

	buf := bytes.Repeat([]byte{0xFF}, 8)

	err := dm.ReadPage(5, buf)
	require.NoError(t, err)
	require.Equal(t, make([]byte, 8), buf)
}

func TestDiskManager_WrongBufSizeErr(t *testing.T) {
	dm := NewDiskManager(NewMemFile(), 16)

	writeErr := dm.WritePage(0, make([]byte, 15))
	require.Error(t, writeErr)

	readErr := dm.ReadPage(0, make([]byte, 15))
	require.Error(t, readErr)
}

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
