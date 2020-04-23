package bitcask

import (
	"bytes"
	"testing"
)

func TestPut(t *testing.T) {
	db, err := Open("/tmp/db")
	if err != nil {
		t.Errorf("test put error: %v", err)
	}
	defer db.Close()
	db.Put([]byte("hello"), []byte("world"))
	got, err := db.Get([]byte("hello"))
	if !bytes.Equal(got, []byte("world")) {
		t.Errorf("put error, want: %v, got: %v", []byte("world"), got)
	}
}

func TestGet(t *testing.T) {
	db, err := Open("/tmp/db")
	if err != nil {
		t.Errorf("test put error: %v", err)
	}
	defer db.Close()
	got, err := db.Get([]byte("hello"))
	if !bytes.Equal(got, []byte("world")) {
		t.Errorf("put error, want: %v, got: %v", []byte("world"), got)
	}
}
