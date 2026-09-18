package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

// MANIFEST holds the complete live state; it is rewritten whole and swapped
// in with an atomic rename. Layout:
//
//	version   uint32
//	nextNum   uint64   next SSTable file number
//	lastSeq   uint64   highest sequence in any table
//	logNum    uint64   oldest WAL segment still needed on recovery
//	ntables   uint32
//	table*    num uint64 | size uint64 | count uint64 |
//	          smallest (uvarint len, bytes) | largest (uvarint len, bytes)
//	          newest table first
//	crc32c    uint32 over everything above
const manifestVersion = 1

type manifest struct {
	nextNum uint64
	lastSeq uint64
	logNum  uint64
	tables  []tableMeta
}

var errBadManifest = errors.New("storage: malformed manifest")

func manifestPath(dir string) string {
	return filepath.Join(dir, "MANIFEST")
}

func (m *manifest) encode() []byte {
	b := binary.LittleEndian.AppendUint32(nil, manifestVersion)
	b = binary.LittleEndian.AppendUint64(b, m.nextNum)
	b = binary.LittleEndian.AppendUint64(b, m.lastSeq)
	b = binary.LittleEndian.AppendUint64(b, m.logNum)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.tables)))
	for _, t := range m.tables {
		b = binary.LittleEndian.AppendUint64(b, t.num)
		b = binary.LittleEndian.AppendUint64(b, t.size)
		b = binary.LittleEndian.AppendUint64(b, t.count)
		b = binary.AppendUvarint(b, uint64(len(t.smallest)))
		b = append(b, t.smallest...)
		b = binary.AppendUvarint(b, uint64(len(t.largest)))
		b = append(b, t.largest...)
	}
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

func decodeManifest(b []byte) (*manifest, error) {
	if len(b) < 36 {
		return nil, errBadManifest
	}
	body, sum := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if crc32.Checksum(body, castagnoli) != sum {
		return nil, fmt.Errorf("manifest: %w", ErrCorrupt)
	}
	if v := binary.LittleEndian.Uint32(body); v != manifestVersion {
		return nil, fmt.Errorf("manifest: unsupported version %d", v)
	}
	m := &manifest{
		nextNum: binary.LittleEndian.Uint64(body[4:]),
		lastSeq: binary.LittleEndian.Uint64(body[12:]),
		logNum:  binary.LittleEndian.Uint64(body[20:]),
	}
	n := binary.LittleEndian.Uint32(body[28:])
	p := body[32:]
	readBytes := func() ([]byte, bool) {
		l, k := binary.Uvarint(p)
		if k <= 0 || uint64(len(p)-k) < l {
			return nil, false
		}
		out := append([]byte(nil), p[k:k+int(l)]...)
		p = p[k+int(l):]
		return out, true
	}
	for i := uint32(0); i < n; i++ {
		if len(p) < 24 {
			return nil, errBadManifest
		}
		t := tableMeta{
			num:   binary.LittleEndian.Uint64(p),
			size:  binary.LittleEndian.Uint64(p[8:]),
			count: binary.LittleEndian.Uint64(p[16:]),
		}
		p = p[24:]
		var ok bool
		if t.smallest, ok = readBytes(); !ok {
			return nil, errBadManifest
		}
		if t.largest, ok = readBytes(); !ok {
			return nil, errBadManifest
		}
		m.tables = append(m.tables, t)
	}
	return m, nil
}

func writeManifest(dir string, m *manifest) error {
	tmp := manifestPath(dir) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(m.encode()); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, manifestPath(dir)); err != nil {
		return err
	}
	return syncDir(dir)
}

func readManifest(dir string) (*manifest, error) {
	b, err := os.ReadFile(manifestPath(dir))
	if os.IsNotExist(err) {
		return &manifest{nextNum: 1, logNum: 1}, nil
	}
	if err != nil {
		return nil, err
	}
	return decodeManifest(b)
}
