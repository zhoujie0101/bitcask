package codec

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"jay.com/bitcask/internal"
)

const (
	keySize      = 4
	valueSize    = 8
	checksumSize = 4
)

// Encoder
type Encoder struct {
	w *bufio.Writer
}

// NewEncoder return encoder
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		w: bufio.NewWriter(w),
	}
}

// Encode entry
// msg protocol:
// keyLen | valueLen | key | value | checksum(value)
func (e *Encoder) Encode(entry internal.Entry) (int64, error) {
	sizeBuf := make([]byte, keySize+valueSize)
	binary.BigEndian.PutUint32(sizeBuf[0:keySize], uint32(len(entry.Key)))
	binary.BigEndian.PutUint64(sizeBuf[keySize:keySize+valueSize], uint64(len(entry.Value)))
	if _, err := e.w.Write(sizeBuf); err != nil {
		return 0, errors.Wrap(err, "failed write key & value length prefix")
	}

	if _, err := e.w.Write(entry.Key); err != nil {
		return 0, errors.Wrap(err, "failed write key")
	}

	if _, err := e.w.Write(entry.Value); err != nil {
		return 0, errors.Wrap(err, "failed write value")
	}

	checksumBuf := make([]byte, checksumSize)
	binary.BigEndian.PutUint32(checksumBuf, entry.Checksum)
	if _, err := e.w.Write(checksumBuf); err != nil {
		return 0, errors.Wrap(err, "failed write checksum")
	}
	if err := e.w.Flush(); err != nil {
		return 0, errors.Wrap(err, "failed flush data")
	}
	return int64(keySize + valueSize + len(entry.Key) + len(entry.Value) + checksumSize), nil
}
