package data

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/exp/mmap"
	"jay.com/bitcask/internal"
	"jay.com/bitcask/internal/data/codec"
)

const (
	defaultDatafileFilename = "%09d.data"
)

var (
	errReadOnly  = errors.New("error: read only datafile")
	errReadError = errors.New("error: read error")
)

type DataFile interface {
	FileID() int
	Name() string
	Size() int64
	Sync() error
	Read() (internal.Entry, int64, error)
	ReadAt(offset, size int64) (internal.Entry, error)
	Write(internal.Entry) (int64, int64, error)
	Close() error
}

type datafile struct {
	mu           sync.Mutex
	r            *os.File
	ra           *mmap.ReaderAt
	w            *os.File
	id           int
	offset       int64
	maxKeySize   uint32
	maxValueSize uint64
	enc          *codec.Encoder
	dec          *codec.Decoder
}

func NewDatafile(path string, id int, readonly bool, maxKeySize uint32, maxValueSize uint64) (DataFile, error) {
	var (
		r   *os.File
		ra  *mmap.ReaderAt
		w   *os.File
		err error
	)
	fn := filepath.Join(path, fmt.Sprintf(defaultDatafileFilename, id))
	if !readonly {
		w, err = os.OpenFile(fn, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0640)
		if err != nil {
			return nil, err
		}
	}
	r, err = os.Open(fn)
	if err != nil {
		return nil, err
	}
	ra, err = mmap.Open(fn)
	if err != nil {
		return nil, err
	}
	stat, err := os.Stat(fn)
	if err != nil {
		return nil, err
	}
	offset := stat.Size()
	enc := codec.NewEncoder(w)
	dec := codec.NewDecoder(r, maxKeySize, maxValueSize)

	return &datafile{
		id:           id,
		r:            r,
		w:            w,
		ra:           ra,
		offset:       offset,
		enc:          enc,
		dec:          dec,
		maxKeySize:   maxKeySize,
		maxValueSize: maxValueSize,
	}, nil
}

func (d *datafile) FileID() int {
	return d.id
}

func (d *datafile) Name() string {
	return d.r.Name()
}

func (d *datafile) Size() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.offset
}

func (d *datafile) Sync() error {
	if d.w == nil {
		return errReadOnly
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.w.Sync()
}

func (d *datafile) Read() (e internal.Entry, n int64, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n, err = d.dec.Decode(&e)
	return
}

func (d *datafile) ReadAt(offset, size int64) (e internal.Entry, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	b := make([]byte, size)
	var n int
	if d.w == nil {
		n, err = d.ra.ReadAt(b, offset)
	} else {
		n, err = d.r.ReadAt(b, offset)
	}
	if err != nil {
		return
	}
	if int64(n) != size {
		err = errReadError
		return
	}
	codec.DecodeEntry(b, &e, d.maxKeySize, d.maxValueSize)
	return
}

func (d *datafile) Write(e internal.Entry) (offset int64, size int64, err error) {
	if d.w == nil {
		return -1, 0, errReadOnly
	}
	e.Offset = d.offset
	n, err := d.enc.Encode(e)
	if err != nil {
		return -1, 0, err
	}
	d.offset += n
	return e.Offset, n, nil
}

func (d *datafile) Close() error {
	defer func() {
		d.ra.Close()
		d.r.Close()
	}()
	if d.w == nil {
		return nil
	}
	err := d.Sync()
	if err != nil {
		return err
	}
	return d.w.Close()
}
