package storage

// internalIter walks internal keys in compareInternalKey order. After seek
// or seekToFirst the iterator is positioned at the first key >= target, or
// invalid if none exists.
type internalIter interface {
	seekToFirst()
	seek(ik []byte)
	valid() bool
	next()
	key() []byte
	value() []byte
	close() error
}
