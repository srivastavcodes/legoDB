package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
)

var (
	ErrVersionMismatch = errors.New("version mismatch")
	ErrInvalid         = errors.New("invalid database")
	ErrChecksum        = errors.New("checksum error")
)

// branchPageFlag, leafPageFlag, metaPageFlag, and freelistPageFlag identify
// the type of data stored in a page. They are used as bitmasks on the page
// header flags field.
const (
	branchPageFlag   = 0x01
	leafPageFlag     = 0x02
	metaPageFlag     = 0x04
	freelistPageFlag = 0x10
)

const defaultPageSize = 4096
const version uint32 = 1

const (
	// 4*(magic, version, psize, flags) + 8*(root, flist, pageID, txID, checksum)
	metaPageSize uint32 = 4*4 + 8*5

	// pageID(8b) + flags(2b) + count(2b) + overflow(4b)
	pageHeaderSize uint32 = 8 + 2 + 2 + 4

	// pos(4b) + ksize(4b) + vsize(4b)
	leafPageElementSize uint32 = 4 + 4 + 4

	// ksize(4b) + pos(4b) + pageID(8b)
	branchPageElementSize uint32 = 8 + 4 + 4
)

const magic = 0xED0CDAED

type pgid uint64
type txid uint64

// page represents a single page for the database.
type page struct {
	flags    uint16
	id       pgid
	count    uint16
	overflow uint32
	data     []byte // data is always the full page - header plus body.
}

// branchPageElement is the on-disk header for an internal B+Tree node.
// It records the size and location of the key and the child page whose
// entry is less than its own.
type branchPageElement struct {
	ksize  uint32 // length of the key in bytes.
	pos    uint32 // byte offset from this element to the key data.
	pageID pgid   // child page this element's subtree lives on.
}

// branchPageElement is the on-disk header for an internal B+Tree node.
// It records the size and location of the key/value pair.
type leafPageElement struct {
	pos   uint32 // byte offset from this element to the key data.
	ksize uint32 // length of the key in bytes.
	vsize uint32 // length of the value in bytes.
}

// meta holds the database-level metadata written to page 0 and 1.
// It is the crash recovery anchor: on open, the valid meta-page
// with the highest transaction ID determines the current database
// state.
type meta struct {
	magic    uint32
	version  uint32
	psize    uint32 // page size
	flags    uint32
	root     pgid   // root page of the B+Tree.
	flist    pgid   // page where the freelist is serialized.
	pageID   pgid   // next unallocated page id.
	txID     txid   // transaction id that wrote this meta.
	checksum uint64 // FNV-64a hash of all fields above.
}

func (p *page) encode() {
	idx := 0

	binary.LittleEndian.PutUint16(p.data[idx:], p.flags)
	idx += 2
	binary.LittleEndian.PutUint64(p.data[idx:], uint64(p.id))
	idx += 8
	binary.LittleEndian.PutUint16(p.data[idx:], p.count)
	idx += 2
	binary.LittleEndian.PutUint32(p.data[idx:], p.overflow)
}

func (p *page) decode() {
	idx := 0
	p.flags = binary.LittleEndian.Uint16(p.data[idx : idx+2])
	idx += 2
	p.id = pgid(binary.LittleEndian.Uint64(p.data[idx : idx+8]))
	idx += 8
	p.count = binary.LittleEndian.Uint16(p.data[idx : idx+2])
	idx += 2
	p.overflow = binary.LittleEndian.Uint32(p.data[idx : idx+4])
}

func (p *page) branchPageElement(index uint16) branchPageElement {
	offset := pageHeaderSize + uint32(index)*branchPageElementSize
	return decodeBranchElement(p.data[offset:])
}

func (p *page) branchPageElements() []branchPageElement {
	var branchElements []branchPageElement

	for i := uint32(0); i < uint32(p.count); i++ {
		offset := pageHeaderSize + (i * branchPageElementSize)
		be := decodeBranchElement(p.data[offset:])
		branchElements = append(branchElements, be)
	}
	return branchElements
}

func (p *page) setBranchPageElement(idx uint16, bpe branchPageElement) {
	offset := pageHeaderSize + uint32(idx)*branchPageElementSize
	bpe.encode(p.data[offset:])
}

func (p *page) leafPageElement(index uint16) leafPageElement {
	offset := pageHeaderSize + uint32(index)*leafPageElementSize
	return decodeLeafElement(p.data[offset:])
}

func (p *page) leafPageElements() []leafPageElement {
	var leafElements []leafPageElement

	for i := uint32(0); i < uint32(p.count); i++ {
		offset := pageHeaderSize + (i * leafPageElementSize)
		be := decodeLeafElement(p.data[offset:])
		leafElements = append(leafElements, be)
	}
	return leafElements
}

func (p *page) setLeafPageElement(idx uint16, lpe leafPageElement) {
	offset := pageHeaderSize + uint32(idx)*leafPageElementSize
	lpe.encode(p.data[offset:])
}

// key returns the bytes of the key for this branch element from pageBuf.
// offset is the byte position of this element's header within page.data.
func (be branchPageElement) key(pageBuf []byte, offset int) []byte {
	start := offset + int(be.pos)
	return pageBuf[start : start+int(be.ksize)]
}

func decodeBranchElement(buf []byte) branchPageElement {
	var idx int
	var bpe branchPageElement

	bpe.ksize = binary.LittleEndian.Uint32(buf[idx:])
	idx += 4
	bpe.pos = binary.LittleEndian.Uint32(buf[idx:])
	idx += 4
	bpe.pageID = pgid(binary.LittleEndian.Uint64(buf[idx:]))

	return bpe
}

func (be branchPageElement) encode(buf []byte) {
	var idx int

	binary.LittleEndian.PutUint32(buf[idx:], be.ksize)
	idx += 4
	binary.LittleEndian.PutUint32(buf[idx:], be.pos)
	idx += 4
	binary.LittleEndian.PutUint64(buf[idx:], uint64(be.pageID))
}

func decodeLeafElement(buf []byte) leafPageElement {
	var idx int
	var lpe leafPageElement

	lpe.pos = binary.LittleEndian.Uint32(buf[idx:])
	idx += 4
	lpe.ksize = binary.LittleEndian.Uint32(buf[idx:])
	idx += 4
	lpe.vsize = binary.LittleEndian.Uint32(buf[idx:])

	return lpe
}

func (le leafPageElement) encode(buf []byte) {
	var idx int

	binary.LittleEndian.PutUint32(buf[idx:], le.pos)
	idx += 4
	binary.LittleEndian.PutUint32(buf[idx:], le.ksize)
	idx += 4
	binary.LittleEndian.PutUint32(buf[idx:], le.vsize)
}

// key returns the bytes of the key for this leaf element from pageBuf.
// offset is the byte position of this element's header within page.data.
func (le leafPageElement) key(pageBuf []byte, offset int) []byte {
	start := offset + int(le.pos)
	return pageBuf[start : start+int(le.ksize)]
}

// value returns the bytes of the value for this leaf element from pageBuf.
// offset is the byte position of this element's header within page.data.
func (le leafPageElement) value(pageBuf []byte, offset int) []byte {
	start := offset + int(le.pos) + int(le.ksize)
	return pageBuf[start : start+int(le.vsize)]
}

func (m *meta) sum64() uint64 {
	hash64 := fnv.New64a()
	buf := make([]byte, metaPageSize)
	m.encode(buf)
	_, _ = hash64.Write(buf[:metaPageSize-8])
	return hash64.Sum64()
}

func (m *meta) validate() error {
	if m.version != version {
		return ErrVersionMismatch
	}
	if m.magic != magic {
		return ErrInvalid
	}
	if m.checksum != m.sum64() {
		return ErrChecksum
	}
	return nil
}

func (m *meta) write(p *page) {
	if m.root >= m.pageID {
		panic(fmt.Sprintf(
			"root pgid (%d) above high water mark (%d)",
			m.root, m.pageID,
		))
	}
	if m.flist >= m.pageID {
		panic(fmt.Sprintf(
			"freelist pgid (%d) above high water mark (%d)",
			m.flist, m.pageID,
		))
	}
	// page id is going to be 0 or 1 which we can determine by the transaction ID.
	p.id = pgid(m.txID % 2)
	p.flags |= metaPageFlag

	m.checksum = m.sum64()
	p.encode()
	m.encode(p.data[pageHeaderSize:])
}

func (m *meta) encode(buf []byte) {
	idx := 0
	binary.LittleEndian.PutUint32(buf[idx:], m.magic)
	idx += 4
	binary.LittleEndian.PutUint32(buf[idx:], m.version)
	idx += 4
	binary.LittleEndian.PutUint32(buf[idx:], m.psize)
	idx += 4
	binary.LittleEndian.PutUint32(buf[idx:], m.flags)
	idx += 4
	binary.LittleEndian.PutUint64(buf[idx:], uint64(m.root))
	idx += 8
	binary.LittleEndian.PutUint64(buf[idx:], uint64(m.flist))
	idx += 8
	binary.LittleEndian.PutUint64(buf[idx:], uint64(m.pageID))
	idx += 8
	binary.LittleEndian.PutUint64(buf[idx:], uint64(m.txID))
	idx += 8
	binary.LittleEndian.PutUint64(buf[idx:], m.checksum)
}

func (m *meta) decode(buf []byte) {
	idx := 0
	m.magic = binary.LittleEndian.Uint32(buf[idx:])
	idx += 4
	m.version = binary.LittleEndian.Uint32(buf[idx:])
	idx += 4
	m.psize = binary.LittleEndian.Uint32(buf[idx:])
	idx += 4
	m.flags = binary.LittleEndian.Uint32(buf[idx:])
	idx += 4
	m.root = pgid(binary.LittleEndian.Uint64(buf[idx:]))
	idx += 8
	m.flist = pgid(binary.LittleEndian.Uint64(buf[idx:]))
	idx += 8
	m.pageID = pgid(binary.LittleEndian.Uint64(buf[idx:]))
	idx += 8
	m.txID = txid(binary.LittleEndian.Uint64(buf[idx:]))
	idx += 8
	m.checksum = binary.LittleEndian.Uint64(buf[idx:])
}

type pgids []pgid

func (ids pgids) Len() int           { return len(ids) }
func (ids pgids) Less(i, j int) bool { return ids[i] < ids[j] }
func (ids pgids) Swap(i, j int)      { ids[i], ids[j] = ids[j], ids[i] }

// merge returns the sorted union of ids and ids2.
func (ids pgids) merge(ids2 pgids) pgids {
	if len(ids) == 0 {
		return ids2
	}
	if len(ids2) == 0 {
		return ids
	}
	merged := make(pgids, len(ids)+len(ids2))
	mergePageIds(merged, ids, ids2)
	return merged
}

// mergePageIds copies the sorted union of first and second into the given dst.
// Both first and second must be sorted in ascending order before calling
// this function — the result is undefined if either slice is unsorted.
// dst must have a length of at least len(first)+len(second) or the function
// panics. If either input is empty, the other is copied into dst directly.
// The algorithm is a two-pointer merge that avoids allocating a temporary
// buffer by writing results directly into dst.
func mergePageIds(dst, first, second pgids) {
	if len(dst) < len(first)+len(second) {
		panic(fmt.Sprintf(
			"mergePageIds bad length %d<%d+%d",
			len(dst), len(first), len(second),
		))
	}
	if len(first) == 0 {
		copy(dst, second)
		return
	}
	if len(second) == 0 {
		copy(dst, first)
		return
	}
	merged := dst[:0]

	// assign lead to the slice with a lower starting value, follow to a higher.
	lead, follow := first, second
	if second[0] < first[0] {
		lead, follow = second, first
	}
	// continue while there are elements in the lead.
	for len(lead) > 0 {
		// merge the largest prefix of lead that is less than follow[0].
		idx := sort.Search(len(lead), func(i int) bool {
			return lead[i] > follow[0]
		})
		merged = append(merged, lead[:idx]...)
		if idx >= len(lead) {
			break
		}
		// swap lead and follow since lead has been merged till it was smaller
		// than follow.
		lead, follow = follow, lead[idx:]
	}
	merged = append(merged, follow...)
}
