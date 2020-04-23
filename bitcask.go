package bitcask

import (
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/pkg/errors"
	art "github.com/plar/go-adaptive-radix-tree"
	"jay.com/bitcask/internal"
	"jay.com/bitcask/internal/config"
	"jay.com/bitcask/internal/data"
	"jay.com/bitcask/internal/index"
)

var (
	// ErrKeyNotFound is the error returned when a key is not found
	ErrKeyNotFound = errors.New("error: key not found")

	// ErrKeyTooLarge is the error returned for a key that exceeds the
	// maximum allowed key size (configured with WithMaxKeySize).
	ErrKeyTooLarge = errors.New("error: key too large")

	// ErrValueTooLarge is the error returned for a value that exceeds the
	// maximum allowed value size (configured with WithMaxValueSize).
	ErrValueTooLarge = errors.New("error: value too large")

	// ErrChecksumFailed is the error returned if a key/value retrieved does
	// not match its CRC checksum
	ErrChecksumFailed = errors.New("error: checksum failed")
)

// Bitcask is a struct that represents a on-disk LSM and WAL data structure
// and in-memory hash of key/value pairs as per the Bitcask paper and seen
// in the Riak database.
type Bitcask struct {
	mu        sync.Mutex
	options   []Option
	cfg       *config.Config
	path      string
	curr      data.DataFile
	datafiles map[int]data.DataFile
	indexer   index.Indexer
	t         art.Tree
}

// Open opens the database at the given path with optional options.
// Options can be provided with the `WithXXX` functions that provide
// configuration options as functions.
func Open(path string, options ...Option) (*Bitcask, error) {
	var (
		cfg *config.Config
		err error
	)
	if err = os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	configPath := filepath.Join(path, "config.json")
	if internal.Exists(configPath) {
		if cfg, err = config.Load(configPath); err != nil {
			return nil, err
		}
	} else {
		cfg = newDefaultConfig()
	}

	bitcask := &Bitcask{
		options: options,
		cfg:     cfg,
		path:    path,
		indexer: index.NewIndexer(),
	}

	for _, opt := range options {
		if err = opt(cfg); err != nil {
			return nil, err
		}
	}
	if err = cfg.Save(configPath); err != nil {
		return nil, err
	}

	if err = bitcask.reopen(); err != nil {
		return nil, err
	}

	return bitcask, nil
}

func (b *Bitcask) reopen() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	datafiles, lastID, err := loadDatafiles(b.path, b.cfg.MaxKeySize, b.cfg.MaxValueSize)
	if err != nil {
		return err
	}
	t, err := loadIndex(b.path, b.indexer, b.cfg.MaxKeySize, datafiles)
	if err != nil {
		return err
	}
	curr, err := data.NewDatafile(b.path, lastID, false, b.cfg.MaxKeySize, b.cfg.MaxValueSize)
	if err != nil {
		return err
	}
	b.curr = curr
	b.datafiles = datafiles
	b.t = t
	return nil
}

// Put store key and value in database
// TODO(jay) check whether key exists
func (b *Bitcask) Put(key, value []byte) error {
	if uint32(len(key)) > b.cfg.MaxKeySize {
		return ErrKeyTooLarge
	}
	if uint64(len(value)) > b.cfg.MaxValueSize {
		return ErrValueTooLarge
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	offset, n, err := b.put(key, value)
	if err != nil {
		return err
	}
	item := internal.Item{
		FileID: b.curr.FileID(),
		Offset: offset,
		Size:   n,
	}
	b.t.Insert(key, item)
	return nil
}

// Get retrieves the value of the given key. If the key is not found or an IO
// error occurs a null byte slice is returned along with the error.
func (b *Bitcask) Get(key []byte) ([]byte, error) {
	b.mu.Lock()
	value, found := b.t.Search(key)
	if !found {
		b.mu.Unlock()
		return nil, ErrKeyNotFound
	}
	item := value.(internal.Item)

	var df data.DataFile
	if item.FileID == b.curr.FileID() {
		df = b.curr
	} else {
		df = b.datafiles[item.FileID]
	}
	e, err := df.ReadAt(item.Offset, item.Size)
	b.mu.Unlock()
	if err != nil {
		return nil, err
	}
	checksum := crc32.ChecksumIEEE(e.Value)
	if checksum != e.Checksum {
		return nil, ErrChecksumFailed
	}
	return e.Value, nil
}

// Has return the true if key exists in database, false otherwise
func (b *Bitcask) Has(key []byte) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, found := b.t.Search(key)
	return found
}

// Delete delete the named key, if key not found or an IO error
// occurs the error is returned
func (b *Bitcask) Delete(key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, _, err := b.put(key, []byte{})
	if err != nil {
		return err
	}
	b.t.Delete(key)
	return nil
}

// DeleteAll delete all keys in the database. If an I/O error occurs the error is returned.
func (b *Bitcask) DeleteAll() (err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.t.ForEach(func(node art.Node) (cont bool) {
		_, _, err = b.put(node.Key(), []byte{})
		if err != nil {
			return false
		}
		return true
	})
	b.t = art.New()
	return
}

// Len return the total number of keys in database
func (b *Bitcask) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.t.Size()
}

// Sync flushes all buffers to disk ensuring all data is writing
func (b *Bitcask) Sync() error {
	return b.curr.Sync()
}

// Close close the database
func (b *Bitcask) Close() error {
	if err := b.indexer.Save(b.t, filepath.Join(b.path, "index")); err != nil {
		return err
	}
	for _, f := range b.datafiles {
		err := f.Close()
		if err != nil {
			return err
		}
	}
	return b.curr.Close()
}

func (b *Bitcask) put(key, value []byte) (int64, int64, error) {
	size := b.curr.Size()
	// TODO make new datafile
	if size > int64(b.cfg.MaxDatafileSize) {
		b.curr.Close()
		id := b.curr.FileID()
		datafile, err := data.NewDatafile(b.path, id, true, b.cfg.MaxKeySize, b.cfg.MaxValueSize)
		if err != nil {
			return -1, 0, err
		}
		b.datafiles[id] = datafile

		datafile, err = data.NewDatafile(b.path, id+1, false, b.cfg.MaxKeySize, b.cfg.MaxValueSize)
		if err != nil {
			return -1, 0, err
		}
		b.curr = datafile
	}
	e := internal.NewEntry(key, value)
	return b.curr.Write(e)
}

func loadDatafiles(path string, maxKeySize uint32, maxValueSize uint64) (datafiles map[int]data.DataFile, lastID int, err error) {
	fns, err := internal.GetDatafiles(path)
	if err != nil {
		return nil, 0, err
	}
	ids, err := internal.ParseIds(fns)
	if err != nil {
		return nil, 0, err
	}
	datafiles = make(map[int]data.DataFile)
	for _, id := range ids {
		file, err := data.NewDatafile(path, id, true, maxKeySize, maxValueSize)
		if err != nil {
			return nil, 0, err
		}
		datafiles[id] = file
	}
	if len(ids) > 0 {
		lastID = ids[len(ids)-1]
	}
	return
}

func loadIndex(path string, indexer index.Indexer, maxKeySize uint32, datafles map[int]data.DataFile) (art.Tree, error) {
	t, found, err := indexer.Load(filepath.Join(path, "index"), maxKeySize)
	if err != nil {
		return nil, err
	}
	if !found {
		sortedDatafiles := getSortedDatafiles(datafles)
		var offset int64
		for _, f := range sortedDatafiles {
			e, n, err := f.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}
			//tombstome
			if len(e.Value) == 0 {
				t.Delete(e.Key)
				offset += n
				continue
			}
			item := internal.Item{
				FileID: f.FileID(),
				Offset: offset,
				Size:   n,
			}
			t.Insert(e.Key, item)
			offset += n
		}
	}
	return t, nil
}

func getSortedDatafiles(datafles map[int]data.DataFile) []data.DataFile {
	files := make([]data.DataFile, len(datafles))
	i := 0
	for _, f := range datafles {
		files[i] = f
		i++
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].FileID() < files[j].FileID()
	})
	return files
}
