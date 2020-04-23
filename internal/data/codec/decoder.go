package codec

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"jay.com/bitcask/internal"
)

var (
	errInvalidKeyOrValueSize = errors.New("key/value size is invalid")
	errCantDecodeOnNilEntry  = errors.New("can't decode on nil entry")
	errTruncatedData         = errors.New("data is truncated")
)

type Decoder struct {
	r            io.Reader
	maxKeySize   uint32
	maxValueSize uint64
}

func NewDecoder(r io.Reader, maxKeySize uint32, maxValueSize uint64) *Decoder {
	return &Decoder{
		r:            r,
		maxKeySize:   maxKeySize,
		maxValueSize: maxValueSize,
	}
}

func (d *Decoder) Decode(e *internal.Entry) (int64, error) {
	if e == nil {
		return 0, errCantDecodeOnNilEntry
	}
	keyValueSizeBuf := make([]byte, keySize+valueSize)
	if _, err := io.ReadFull(d.r, keyValueSizeBuf); err != nil {
		return 0, err
	}
	actualKeySize, actualValueSize, err := getKeyValueSizes(keyValueSizeBuf, d.maxKeySize, d.maxValueSize)
	if err != nil {
		return 0, err
	}
	buf := make([]byte, uint64(actualKeySize)+actualValueSize+checksumSize)
	if _, err := io.ReadFull(d.r, buf); err != nil {
		return 0, errTruncatedData
	}
	decodeWithoutPrefix(buf, actualKeySize, e)
	return int64(keySize + valueSize + uint64(actualKeySize) + actualValueSize + checksumSize), nil
}

func DecodeEntry(b []byte, e *internal.Entry, maxKeySize uint32, maxValueSize uint64) error {
	actualKeySize, _, err := getKeyValueSizes(b, maxKeySize, maxValueSize)
	if err != nil {
		return errors.Wrap(err, "key/value sizes are invalid")
	}
	decodeWithoutPrefix(b[keySize+valueSize:], actualKeySize, e)
	return nil
}

func getKeyValueSizes(b []byte, maxKeySize uint32, maxValueSize uint64) (uint32, uint64, error) {
	actualKeySize := binary.BigEndian.Uint32(b[:keySize])
	actualValueSize := binary.BigEndian.Uint64(b[keySize:])
	if actualKeySize > maxKeySize || actualValueSize > maxValueSize || actualKeySize == 0 {
		return 0, 0, errInvalidKeyOrValueSize
	}

	return actualKeySize, actualValueSize, nil
}

func decodeWithoutPrefix(b []byte, actualKeySize uint32, e *internal.Entry) {
	e.Key = b[:actualKeySize]
	e.Value = b[actualKeySize : len(b)-checksumSize]
	e.Checksum = binary.BigEndian.Uint32(b[len(b)-checksumSize:])
}
