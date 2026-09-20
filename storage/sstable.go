package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

// SSTable file layout:
//
//	[data block][crc32c]*     sorted internal keys, see block.go
//	[index block][crc32c]     key = last key of each data block,
//	                          value = uvarint offset, uvarint size
//	[bloom filter][crc32c]    over user keys, see bloom.go
//	[footer]                  40 bytes, fixed
//
// Footer:
//
//	index offset  uint64   bloom offset  uint64
//	index size    uint64   bloom size    uint64
//	version       uint32   magic         uint32
//
// Every block's checksum covers the block bytes that precede it. Sizes in
// handles exclude the 4-byte checksum.
const (
	footerSize   = 40
	tableMagic   = 0x4b455953
	tableVersion = 1
)

var errBadTable = errors.New("sstable: malformed file")

type blockHandle struct {
	offset uint64
	size   uint64
}

func (h blockHandle) encode(dst []byte) []byte {
	dst = binary.AppendUvarint(dst, h.offset)
	return binary.AppendUvarint(dst, h.size)
}

func decodeHandle(b []byte) (blockHandle, bool) {
	off, n1 := binary.Uvarint(b)
	size, n2 := binary.Uvarint(b[n1:])
	return blockHandle{off, size}, n1 > 0 && n2 > 0
}

type tableMeta struct {
	num      uint64
	size     uint64
	count    uint64
	smallest []byte
	largest  []byte
}

func tablePath(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%08d.sst", num))
}

type tableWriter struct {
	f         *os.File
	w         *bufio.Writer
	off       uint64
	blockSize int
	data      blockBuilder
	index     blockBuilder
	bloomKeys [][]byte
	meta      tableMeta
	pending   bool
	lastKey   []byte
	handle    blockHandle
}

func newTableWriter(path string, num uint64, blockSize int) (*tableWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &tableWriter{f: f, w: bufio.NewWriterSize(f, 1<<20), blockSize: blockSize, meta: tableMeta{num: num}}, nil
}

// newStreamWriter writes a table to w with no file behind it; finish
// flushes but has nothing to sync or close.
func newStreamWriter(w io.Writer, blockSize int) *tableWriter {
	return &tableWriter{w: bufio.NewWriterSize(w, 1<<20), blockSize: blockSize}
}

func (w *tableWriter) add(ik, value []byte) error {
	if w.pending {
		w.index.add(w.lastKey, w.handle.encode(nil))
		w.pending = false
	}
	if w.meta.count == 0 {
		w.meta.smallest = append([]byte(nil), ik...)
	}
	w.data.add(ik, value)
	w.lastKey = append(w.lastKey[:0], ik...)
	w.bloomKeys = append(w.bloomKeys, append([]byte(nil), userKey(ik)...))
	w.meta.count++
	if w.data.size() >= w.blockSize {
		return w.flushBlock()
	}
	return nil
}

func (w *tableWriter) flushBlock() error {
	if w.data.empty() {
		return nil
	}
	h, err := w.writeBlock(w.data.finish())
	if err != nil {
		return err
	}
	w.data.reset()
	w.handle = h
	w.pending = true
	return nil
}

func (w *tableWriter) writeBlock(b []byte) (blockHandle, error) {
	h := blockHandle{offset: w.off, size: uint64(len(b))}
	if _, err := w.w.Write(b); err != nil {
		return h, err
	}
	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], crc32.Checksum(b, castagnoli))
	if _, err := w.w.Write(crc[:]); err != nil {
		return h, err
	}
	w.off += uint64(len(b)) + 4
	return h, nil
}

// finish writes the index, bloom filter and footer, then syncs and closes
// the file. The returned metadata describes the completed table.
func (w *tableWriter) finish() (tableMeta, error) {
	if err := w.flushBlock(); err != nil {
		return tableMeta{}, err
	}
	if w.pending {
		w.index.add(w.lastKey, w.handle.encode(nil))
	}
	indexHandle, err := w.writeBlock(w.index.finish())
	if err != nil {
		return tableMeta{}, err
	}
	bloomHandle, err := w.writeBlock(buildBloom(w.bloomKeys))
	if err != nil {
		return tableMeta{}, err
	}
	var footer [footerSize]byte
	binary.LittleEndian.PutUint64(footer[0:], indexHandle.offset)
	binary.LittleEndian.PutUint64(footer[8:], indexHandle.size)
	binary.LittleEndian.PutUint64(footer[16:], bloomHandle.offset)
	binary.LittleEndian.PutUint64(footer[24:], bloomHandle.size)
	binary.LittleEndian.PutUint32(footer[32:], tableVersion)
	binary.LittleEndian.PutUint32(footer[36:], tableMagic)
	if _, err := w.w.Write(footer[:]); err != nil {
		return tableMeta{}, err
	}
	if err := w.w.Flush(); err != nil {
		return tableMeta{}, err
	}
	if w.f != nil {
		if err := w.f.Sync(); err != nil {
			return tableMeta{}, err
		}
		if err := w.f.Close(); err != nil {
			return tableMeta{}, err
		}
	}
	w.meta.size = w.off + footerSize
	w.meta.largest = append([]byte(nil), w.lastKey...)
	return w.meta, nil
}

func (w *tableWriter) abort() {
	if w.f != nil {
		w.f.Close()
		os.Remove(w.f.Name())
	}
}

// table is an open SSTable. Readers hold a reference so a compaction can
// unlink the file while iterators are still reading it.
type table struct {
	meta  tableMeta
	r     io.ReaderAt
	size  int64
	path  string
	close func() error
	index []byte
	bloom bloomFilter
	refs  atomic.Int32
	gone  atomic.Bool
}

func openTable(path string, meta tableMeta) (*table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	t := &table{meta: meta, r: f, size: info.Size(), path: path, close: f.Close}
	t.refs.Store(1)
	if err := t.readFooter(); err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return t, nil
}

// openTableBytes reads a table held in memory, as produced by Export.
func openTableBytes(b []byte) (*table, error) {
	t := &table{r: bytes.NewReader(b), size: int64(len(b)), close: func() error { return nil }}
	t.refs.Store(1)
	if err := t.readFooter(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *table) readFooter() error {
	if t.size < footerSize {
		return errBadTable
	}
	var footer [footerSize]byte
	if _, err := t.r.ReadAt(footer[:], t.size-footerSize); err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(footer[36:]) != tableMagic {
		return errBadTable
	}
	if v := binary.LittleEndian.Uint32(footer[32:]); v != tableVersion {
		return fmt.Errorf("sstable: unsupported version %d", v)
	}
	indexHandle := blockHandle{binary.LittleEndian.Uint64(footer[0:]), binary.LittleEndian.Uint64(footer[8:])}
	bloomHandle := blockHandle{binary.LittleEndian.Uint64(footer[16:]), binary.LittleEndian.Uint64(footer[24:])}
	index, err := t.readBlock(indexHandle)
	if err != nil {
		return err
	}
	t.index = index
	bloom, err := t.readBlock(bloomHandle)
	if err != nil {
		return err
	}
	t.bloom = bloomFilter(bloom)
	return nil
}

func (t *table) readBlock(h blockHandle) ([]byte, error) {
	// The footer carries no checksum, and make panics on a wild size.
	if h.offset > uint64(t.size) || h.size > uint64(t.size)-h.offset {
		return nil, errBadTable
	}
	buf := make([]byte, h.size+4)
	if _, err := t.r.ReadAt(buf, int64(h.offset)); err != nil {
		if err == io.EOF {
			return nil, errBadTable
		}
		return nil, err
	}
	if crc32.Checksum(buf[:h.size], castagnoli) != binary.LittleEndian.Uint32(buf[h.size:]) {
		return nil, fmt.Errorf("block at %d: %w", h.offset, ErrCorrupt)
	}
	return buf[:h.size], nil
}

func (t *table) ref() {
	t.refs.Add(1)
}

// unref closes the file when the last reference drops, and unlinks it if
// the table was removed from the live set meanwhile.
func (t *table) unref() {
	if t.refs.Add(-1) != 0 {
		return
	}
	t.close()
	if t.gone.Load() && t.path != "" {
		os.Remove(t.path)
	}
}

// get returns the newest version of key with sequence <= seq.
func (t *table) get(key []byte, seq uint64) (value []byte, found, deleted bool, err error) {
	if !t.bloom.mayContain(key) {
		return nil, false, false, nil
	}
	it := t.iter()
	defer it.close()
	it.seek(makeInternalKey(nil, key, seq, kindPut))
	if !it.valid() {
		return nil, false, false, it.close()
	}
	user, _, kind := splitInternalKey(it.key())
	if string(user) != string(key) {
		return nil, false, false, nil
	}
	if kind == kindDelete {
		return nil, true, true, nil
	}
	return append([]byte(nil), it.value()...), true, false, nil
}

func (t *table) iter() *tableIter {
	t.ref()
	index, err := newBlockIter(t.index)
	return &tableIter{t: t, index: index, err: err}
}

type tableIter struct {
	t     *table
	index *blockIter
	block *blockIter
	err   error
}

func (it *tableIter) loadBlock() bool {
	if it.err != nil || !it.index.valid() {
		it.block = nil
		return false
	}
	h, ok := decodeHandle(it.index.value())
	if !ok {
		it.err = errBadTable
		return false
	}
	data, err := it.t.readBlock(h)
	if err != nil {
		it.err = err
		return false
	}
	it.block, it.err = newBlockIter(data)
	return it.err == nil
}

func (it *tableIter) seekToFirst() {
	if it.err != nil {
		return
	}
	it.index.seekToFirst()
	if it.loadBlock() {
		it.block.seekToFirst()
		it.skipExhausted()
	}
}

func (it *tableIter) seek(target []byte) {
	if it.err != nil {
		return
	}
	it.index.seek(target)
	if it.loadBlock() {
		it.block.seek(target)
		it.skipExhausted()
	}
}

func (it *tableIter) valid() bool {
	return it.err == nil && it.block != nil && it.block.valid()
}

func (it *tableIter) next() {
	it.block.next()
	it.skipExhausted()
}

// skipExhausted moves to the next data block while the current one has no
// entries left.
func (it *tableIter) skipExhausted() {
	for it.block != nil && !it.block.valid() && it.err == nil {
		if it.block.err != nil {
			it.err = it.block.err
			return
		}
		it.index.next()
		if !it.loadBlock() {
			return
		}
		it.block.seekToFirst()
	}
}

func (it *tableIter) key() []byte   { return it.block.key() }
func (it *tableIter) value() []byte { return it.block.value() }

func (it *tableIter) close() error {
	if it.t != nil {
		it.t.unref()
		it.t = nil
	}
	return it.err
}
