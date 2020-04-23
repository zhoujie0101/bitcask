package index

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
	art "github.com/plar/go-adaptive-radix-tree"
	"jay.com/bitcask/internal"
)

const (
	int32Size  = 4
	int64Size  = 8
	fileIDSize = int32Size
	offsetSize = int64Size
	sizeSize   = int64Size
)

var (
	errTruncatedKeySize = errors.New("key size is truncated")
	errTruncatedKeyData = errors.New("key data is truncated")
	errTruncatedData    = errors.New("data is truncated")
	errKeySizeTooLarge  = errors.New("key size too large")
)

type Indexer interface {
	Load(path string, maxKeySize uint32) (art.Tree, bool, error)
	Save(t art.Tree, path string) error
}

func NewIndexer() *indexer {
	return &indexer{}
}

type indexer struct {
}

func (i *indexer) Load(path string, maxKeySize uint32) (art.Tree, bool, error) {
	fmt.Println(path)
	t := art.New()
	if !internal.Exists(path) {
		return t, false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return t, true, err
	}
	if err := readIndex(t, f, maxKeySize); err != nil {
		return t, true, err
	}
	return t, true, nil
}

func (i *indexer) Save(t art.Tree, path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := writeIndex(t, f); err != nil {
		return err
	}
	return f.Sync()
}

func writeIndex(t art.Tree, w io.Writer) (err error) {
	t.ForEach(func(node art.Node) (cont bool) {
		err = writeKey(node.Key(), w)
		if err != nil {
			return false
		}
		item := node.Value().(internal.Item)
		err = writeItem(item, w)
		if err != nil {
			return false
		}
		return true
	})
	return
}

func readIndex(t art.Tree, r io.Reader, maxKeySize uint32) error {
	for {
		key, err := readKey(r, maxKeySize)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		item, err := readItem(r)
		if err != nil {
			return err
		}
		t.Insert(key, item)
	}
	return nil
}

func writeKey(b []byte, w io.Writer) error {
	size := make([]byte, int32Size)
	binary.BigEndian.PutUint32(size, uint32(len(b)))
	if _, err := w.Write(size); err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	return nil
}

func readKey(r io.Reader, maxKeySize uint32) ([]byte, error) {
	keySizeBuf := make([]byte, int32Size)
	_, err := io.ReadFull(r, keySizeBuf)
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, errors.Wrap(errTruncatedKeySize, err.Error())
	}
	size := binary.BigEndian.Uint32(keySizeBuf)
	if size > maxKeySize {
		return nil, errKeySizeTooLarge
	}

	keyBuf := make([]byte, size)
	_, err = io.ReadFull(r, keyBuf)
	if err != nil {
		return nil, errors.Wrap(errTruncatedKeyData, err.Error())
	}
	return keyBuf, nil
}

func writeItem(i internal.Item, w io.Writer) error {
	buf := make([]byte, fileIDSize+offsetSize+sizeSize)
	binary.BigEndian.PutUint32(buf[:fileIDSize], uint32(i.FileID))
	binary.BigEndian.PutUint64(buf[fileIDSize:fileIDSize+offsetSize], uint64(i.Offset))
	binary.BigEndian.PutUint64(buf[fileIDSize+offsetSize:], uint64(i.Size))
	if _, err := w.Write(buf); err != nil {
		return err
	}
	return nil
}

func readItem(r io.Reader) (internal.Item, error) {
	buf := make([]byte, fileIDSize+offsetSize+sizeSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return internal.Item{}, errors.Wrap(errTruncatedData, err.Error())
	}
	return internal.Item{
		FileID: int(binary.BigEndian.Uint32(buf[:fileIDSize])),
		Offset: int64(binary.BigEndian.Uint64(buf[fileIDSize : fileIDSize+offsetSize])),
		Size:   int64(binary.BigEndian.Uint64(buf[fileIDSize+offsetSize:])),
	}, nil
}
