package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/rs/zerolog/log"
)

// FileHandler covers the operations that os.File provides but that we need to be
// able to fake in tests.
type FileHandler interface {
	// ReadAt reads len(buf) bytes from the store starting at the byte offset off.
	// It returns the number of bytes read and an error, if any. It returns a
	// non-nil error if n < len(buf). At the end of the file that error is io.EOF.
	ReadAt(buf []byte, off int64) (n int, err error)

	// WriteAt writes len(buf) bytes to the store starting at the byte offset off.
	// It returns the number of bytes written and an error, if any. It returns a
	// non-nil error if n != len(buf).
	WriteAt(buf []byte, off int64) (n int, err error)

	// Sync commits the in-memory contents of the store to disk.
	Sync() error

	// Truncate changes the size of the file. It does not change the I/O offsets.
	Truncate(size int64) error

	// Close closes the underlying store.
	Close() error
}

// DiskManager wraps a FileHandler and exposes page-number-based access.
type DiskManager struct {
	store    FileHandler // store provides file or file like operations
	pageSize int         // size of every page in the store read at once.
}

// NewDiskManager returns a pointer to an initialized DiskManager.
func NewDiskManager(file FileHandler, pageSize int) *DiskManager {
	return &DiskManager{
		store: file, pageSize: pageSize,
	}
}

// ReadPage reads exactly one page from disk into buf, which must be exactly
// pageSize bytes, if not, an error is returned. An EOF mid-read means the
// page was never written, in which case buf is zeroed out and no error is
// returned. Any other error is returned if encountered.
func (dm *DiskManager) ReadPage(pageNum uint32, buf []byte) (err error) {
	if len(buf) != dm.pageSize {
		return fmt.Errorf(
			"incorrect length buffer. want=%d, got=%d",
			dm.pageSize, len(buf),
		)
	}
	off := int64(dm.pageSize) * int64(pageNum)
	// a partial read at the end means the data was never written.
	// pager handles this case.
	if _, err = dm.store.ReadAt(buf, off); errors.Is(err, io.EOF) {
		clear(buf)
		return nil
	}
	return err
}

// WritePage writes exactly one page from buf to disk at offset pageNum * pageSize.
// buf must be exactly pageSize bytes, if not, an error is returned. The write is
// not guaranteed to reach durable storage until Sync is called.
func (dm *DiskManager) WritePage(pageNum uint32, buf []byte) error {
	if len(buf) != dm.pageSize {
		return fmt.Errorf(
			"incorrect length buffer. want=%d, got=%d",
			dm.pageSize, len(buf),
		)
	}
	offset := int64(dm.pageSize) * int64(pageNum)
	_, err := dm.store.WriteAt(buf, offset)
	return err
}

// Sync flushes any OS-buffered writes to durable storage.
func (dm *DiskManager) Sync() error { return dm.store.Sync() }

// OSFile is a separate type just to add the Size() method directly to the disk
// operations and satisfy the interface FileHandler. Otherwise, you would have to
// call fd.Stat() every time you needed the size, this simplifies that, other than
// that, it's just a file.
type OSFile struct {
	fd *os.File
}

// OpenFile opens the file for read/write and creates one if not exist with the
// provided name. It returns a pointer to an initialized OSFile or an error
// if the file could not be opened.
func OpenFile(name string) (*OSFile, error) {
	fd, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, err
	}
	return &OSFile{fd: fd}, nil
}

func (odf *OSFile) ReadAt(buf []byte, off int64) (int, error) {
	return odf.fd.ReadAt(buf, off)
}

func (odf *OSFile) WriteAt(buf []byte, off int64) (int, error) {
	return odf.fd.WriteAt(buf, off)
}

func (odf *OSFile) Sync() error { return odf.fd.Sync() }

func (odf *OSFile) Truncate(size int64) error {
	return odf.fd.Truncate(size)
}

func (odf *OSFile) Close() error { return odf.fd.Close() }

func (odf *OSFile) Size() int64 {
	info, err := odf.fd.Stat()
	if err != nil {
		log.Error().Msg("couldn't stat file")
		return 0
	}
	return info.Size()
}

// MemFile is the in-memory test double. It satisfies the FileHandler interface.
// Every test for the storage layer uses this instead of a real file to simulate
// conditions that may not be possible with the on disk file.
type MemFile struct {
	mu  sync.Mutex
	buf []byte
}

// NewMemFile returns a pointer to an initialized zero value MemFile.
func NewMemFile() *MemFile { return new(MemFile) }

func (mf *MemFile) ReadAt(buf []byte, off int64) (int, error) {
	mf.mu.Lock()
	defer mf.mu.Unlock()
	if int(off) >= len(mf.buf) {
		return 0, io.EOF
	}
	n := copy(buf, mf.buf[off:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

func (mf *MemFile) WriteAt(buf []byte, off int64) (int, error) {
	mf.mu.Lock()
	defer mf.mu.Unlock()
	end := len(buf) + int(off)
	if end > len(mf.buf) {
		mf.buf = append(mf.buf, make([]byte, end-len(mf.buf))...)
	}
	n := copy(mf.buf[off:], buf)
	if n != len(buf) {
		return n, fmt.Errorf(
			"bytes written does not equal buffer length. want=%d, got=%d",
			len(buf), n,
		)
	}
	return n, nil
}

func (mf *MemFile) Sync() error { return nil }

func (mf *MemFile) Close() error { return nil }

func (mf *MemFile) Truncate(size int64) error {
	mf.mu.Lock()
	defer mf.mu.Unlock()

	if int(size) > len(mf.buf) {
		mf.buf = append(mf.buf, make([]byte, int(size)-len(mf.buf))...)
	} else {
		mf.buf = mf.buf[:size]
	}
	return nil
}

func (mf *MemFile) Size() int64 {
	mf.mu.Lock()
	defer mf.mu.Unlock()
	return int64(len(mf.buf))
}
