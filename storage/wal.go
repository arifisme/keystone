package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SyncPolicy decides when appended records reach stable storage. The zero
// value syncs on every append.
type SyncPolicy struct {
	Interval time.Duration
}

var SyncAlways = SyncPolicy{}

func SyncInterval(d time.Duration) SyncPolicy {
	return SyncPolicy{Interval: d}
}

type WALOptions struct {
	// SegmentSize is the size at which Append starts a new segment file.
	SegmentSize int64
	Sync        SyncPolicy
	// OnSync, if set, is told how long each fsync took.
	OnSync func(time.Duration)
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Record layout inside a segment:
//
//	length  uint32 little-endian, payload bytes
//	crc     uint32 little-endian, CRC-32C of payload
//	payload
//
// Segments are named by an increasing number. A segment is never rewritten;
// the only mutation after a crash is truncating a torn final record.
const recordHeader = 8

var ErrCorrupt = errors.New("wal: corrupt record")

// WAL is an append-only log of opaque records split across segment files.
// It serializes appends and knows nothing about record contents.
type WAL struct {
	dir  string
	opts WALOptions

	mu      sync.Mutex
	file    *os.File
	seg     uint64
	size    int64
	dirty   bool
	closed  bool
	stopSyn chan struct{}
	syncErr error
	// appendErr is set by a failed write, which may have left part of a
	// record in the file. Replay stops at a torn record, so a later append
	// would be acknowledged and then lost; a rotation would leave the torn
	// record in a segment that is no longer allowed one.
	appendErr error
	wg        sync.WaitGroup
}

// OpenWAL replays every record in dir into replay, in order, and then starts
// a fresh segment for new appends. A torn tail in the newest segment is
// truncated; damage anywhere else is reported as ErrCorrupt.
func OpenWAL(dir string, opts WALOptions, replay func(seg uint64, payload []byte) error) (*WAL, error) {
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = 64 << 20
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	segs, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	for i, seg := range segs {
		last := i == len(segs)-1
		if err := replaySegment(segmentPath(dir, seg), seg, last, replay); err != nil {
			return nil, err
		}
	}
	w := &WAL{dir: dir, opts: opts}
	next := uint64(1)
	if len(segs) > 0 {
		next = segs[len(segs)-1] + 1
	}
	if err := w.openSegment(next); err != nil {
		return nil, err
	}
	if opts.Sync.Interval > 0 {
		w.stopSyn = make(chan struct{})
		w.wg.Add(1)
		go w.syncLoop()
	}
	return w, nil
}

func listSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []uint64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".wal") {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(name, ".wal"), 10, 64)
		if err != nil {
			continue
		}
		segs = append(segs, n)
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i] < segs[j] })
	return segs, nil
}

func segmentPath(dir string, seg uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%08d.wal", seg))
}

func replaySegment(path string, seg uint64, last bool, replay func(uint64, []byte) error) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	var (
		off    int64
		header [recordHeader]byte
		buf    []byte
	)
	for {
		if _, err := io.ReadFull(f, header[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return truncateTorn(f, path, off, last, err)
		}
		length := binary.LittleEndian.Uint32(header[0:4])
		crc := binary.LittleEndian.Uint32(header[4:8])
		if length == 0 {
			return truncateTorn(f, path, off, last, ErrCorrupt)
		}
		if int(length) > cap(buf) {
			buf = make([]byte, length)
		}
		buf = buf[:length]
		if _, err := io.ReadFull(f, buf); err != nil {
			return truncateTorn(f, path, off, last, err)
		}
		if crc32.Checksum(buf, castagnoli) != crc {
			return truncateTorn(f, path, off, last, ErrCorrupt)
		}
		if err := replay(seg, buf); err != nil {
			return err
		}
		off += recordHeader + int64(length)
	}
}

func truncateTorn(f *os.File, path string, off int64, last bool, cause error) error {
	if !last {
		return fmt.Errorf("%s at offset %d: %w", path, off, ErrCorrupt)
	}
	if err := f.Truncate(off); err != nil {
		return fmt.Errorf("truncate torn tail of %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if cause != nil && !errors.Is(cause, io.ErrUnexpectedEOF) && !errors.Is(cause, io.EOF) && !errors.Is(cause, ErrCorrupt) {
		return cause
	}
	return nil
}

func (w *WAL) openSegment(seg uint64) error {
	f, err := os.OpenFile(segmentPath(w.dir, seg), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if err := syncDir(w.dir); err != nil {
		f.Close()
		return err
	}
	w.file, w.seg, w.size = f, seg, 0
	return nil
}

// Append writes each payload as its own record, all into the same segment
// and in one write. Under SyncAlways it returns once they are durable.
func (w *WAL) Append(payloads ...[]byte) (seg uint64, err error) {
	total := 0
	for _, p := range payloads {
		if len(p) == 0 {
			return 0, errors.New("wal: empty record")
		}
		total += recordHeader + len(p)
	}
	buf := make([]byte, 0, total)
	for _, p := range payloads {
		var h [recordHeader]byte
		binary.LittleEndian.PutUint32(h[0:4], uint32(len(p)))
		binary.LittleEndian.PutUint32(h[4:8], crc32.Checksum(p, castagnoli))
		buf = append(buf, h[:]...)
		buf = append(buf, p...)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, errors.New("wal: closed")
	}
	if w.appendErr != nil {
		return 0, w.appendErr
	}
	if w.size >= w.opts.SegmentSize {
		if err := w.rotateLocked(); err != nil {
			return 0, err
		}
	}
	if _, err := w.file.Write(buf); err != nil {
		w.appendErr = fmt.Errorf("wal append: %w", err)
		return 0, w.appendErr
	}
	w.size += int64(len(buf))
	w.dirty = true
	if w.opts.Sync.Interval == 0 {
		if err := w.syncLocked(); err != nil {
			return 0, err
		}
	}
	return w.seg, nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncLocked()
}

func (w *WAL) syncLocked() error {
	if !w.dirty {
		return w.syncErr
	}
	start := time.Now()
	if err := w.file.Sync(); err != nil {
		w.syncErr = fmt.Errorf("wal sync: %w", err)
		return w.syncErr
	}
	if w.opts.OnSync != nil {
		w.opts.OnSync(time.Since(start))
	}
	w.dirty = false
	return nil
}

func (w *WAL) syncLoop() {
	defer w.wg.Done()
	t := time.NewTicker(w.opts.Sync.Interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			w.Sync()
		case <-w.stopSyn:
			return
		}
	}
}

// Rotate closes the current segment and starts the next one. Records
// appended afterwards report the new segment number.
func (w *WAL) Rotate() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.rotateLocked(); err != nil {
		return 0, err
	}
	return w.seg, nil
}

func (w *WAL) rotateLocked() error {
	if w.appendErr != nil {
		return w.appendErr
	}
	if err := w.syncLocked(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	return w.openSegment(w.seg + 1)
}

func (w *WAL) Segment() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seg
}

func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// DeleteBefore removes every segment numbered below seg.
func (w *WAL) DeleteBefore(seg uint64) error {
	segs, err := listSegments(w.dir)
	if err != nil {
		return err
	}
	for _, s := range segs {
		if s >= seg {
			break
		}
		if err := os.Remove(segmentPath(w.dir, s)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.mu.Unlock()
	if w.stopSyn != nil {
		close(w.stopSyn)
		w.wg.Wait()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.syncLocked()
	if cerr := w.file.Close(); err == nil {
		err = cerr
	}
	return err
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
