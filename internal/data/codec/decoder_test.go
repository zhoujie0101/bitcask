package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"jay.com/bitcask/internal"
)

func TestDecodeOnNilEntry(t *testing.T) {
	d := NewDecoder(&bytes.Buffer{}, 1, 1)
	_, err := d.Decode(nil)
	if !errors.Is(err, errCantDecodeOnNilEntry) {
		t.Errorf("expected: %v, but got: %v", errCantDecodeOnNilEntry, err)
	}
}

func TestShortPrefix(t *testing.T) {
	b := make([]byte, keySize+valueSize)
	binary.BigEndian.PutUint32(b, 1)
	binary.BigEndian.PutUint64(b[keySize:], 1)
	trancate := 2
	buf := bytes.NewBuffer(b[0 : len(b)-trancate])
	d := NewDecoder(buf, keySize, valueSize)
	_, err := d.Decode(&internal.Entry{})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("expected: %v, but got: %v", io.ErrUnexpectedEOF, err)
	}
}

func TestInvalidValueKeySizes(t *testing.T) {
	maxKeySize, maxValueSize := uint32(10), uint64(20)
	tests := []struct {
		keySize   uint32
		valueSize uint64
		name      string
	}{
		{keySize: 0, valueSize: 5, name: "zero key size"},
		{keySize: 11, valueSize: 5, name: "key size overflow"},
		{keySize: 1, valueSize: 25, name: "value size overflow"},
		{keySize: 11, valueSize: 25, name: "key and value size overflow"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			prefix := make([]byte, keySize+valueSize)
			binary.BigEndian.PutUint32(prefix, test.keySize)
			binary.BigEndian.PutUint64(prefix[keySize:], test.valueSize)
			buf := bytes.NewBuffer(prefix)
			decoder := NewDecoder(buf, maxKeySize, maxValueSize)
			_, err := decoder.Decode(&internal.Entry{})
			if !errors.Is(err, errInvalidKeyOrValueSize) {
				t.Errorf("expected: %v, but got: %v", errInvalidKeyOrValueSize, err)
			}
		})
	}
}
