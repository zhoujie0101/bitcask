package internal

import "hash/crc32"

// Entry wrap key, value, offset and value checksum
type Entry struct {
	Checksum uint32
	Key      []byte
	Offset   int64
	Value    []byte
}

// NewEntry return new entry
func NewEntry(key, value []byte) Entry {
	checksum := crc32.ChecksumIEEE(value)
	return Entry{
		Checksum: checksum,
		Key:      key,
		Value:    value,
	}
}
