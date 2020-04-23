package codec

import (
	"bytes"
	"testing"

	"jay.com/bitcask/internal"
)

func TestEncode(t *testing.T) {
	key := []byte("mykey")
	value := []byte("myvalue")

	entry := internal.NewEntry(key, value)
	var buf bytes.Buffer
	encoder := NewEncoder(&buf)
	n, err := encoder.Encode(entry)
	if err != nil {
		t.Errorf("encode err : %v", err)
		return
	}
	want := 4 + 8 + len(key) + len(value) + 4
	if n != int64(want) {
		t.Errorf("encode size err, want: %d, got: %d", n, want)
	}

	keySize := make([]byte, 4)
	kn, err := buf.Read(keySize)
	if kn != 4 {
		t.Errorf("keysize error, want: %d, got: %d", keySize, kn)
	}

	valueSize := make([]byte, 8)
	vn, err := buf.Read(valueSize)
	if vn != 8 {
		t.Errorf("keysize error, want: %d, got: %d", value, vn)
	}

	readKey := make([]byte, len(key))
	rkn, err := buf.Read(readKey)
	if rkn != len(key) {
		t.Errorf("key size error, want: %d, got: %d", len(key), rkn)
	}
	if bytes.Compare(key, readKey) != 0 {
		t.Errorf("key error, want: %v, got: %v", key, readKey)
	}

	readValue := make([]byte, len(value))
	rvn, err := buf.Read(readValue)
	if rvn != len(value) {
		t.Errorf("value size error, want: %d, got: %d", len(value), rvn)
	}
	if bytes.Compare(value, readValue) != 0 {
		t.Errorf("key error, want: %v, got: %v", value, readValue)
	}
}
