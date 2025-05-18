package datastore

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const (
	baseFilename = "current-data-"
	maxSize      = 10 * 1024 * 1024
	fileFlags    = os.O_RDWR | os.O_CREATE
)

const readerLimit = 10

var ErrKeyMissing = errors.New("key not found")

type recordPos struct {
	file   *os.File
	offset int64
}

type Database struct {
	dir     string
	files   []*os.File
	records map[string]recordPos

	mu        sync.RWMutex
	writeChan chan writeRequest
	readChan  chan readRequest
	wg        sync.WaitGroup
}

type writeRequest struct {
	key   string
	value string
	resp  chan error
}

type readRequest struct {
	key  string
	resp chan readResult
}

type readResult struct {
	value string
	err   error
}

func Open(dir string) (*Database, error) {
	db := &Database{
		dir:       dir,
		files:     []*os.File{},
		records:   make(map[string]recordPos),
		writeChan: make(chan writeRequest, 100),
		readChan:  make(chan readRequest, 100),
	}

	matches, _ := filepath.Glob(filepath.Join(dir, baseFilename+"*"))
	for _, path := range matches {
		f, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			db.Close()
			return nil, errors.New("failed to open: " + path)
		}
		db.files = append(db.files, f)
		if err := db.restore(f); err != nil {
			db.Close()
			return nil, err
		}
	}

	if len(db.files) == 0 {
		f, err := db.createNewFile()
		if err != nil {
			return nil, err
		}
		db.files = append(db.files, f)
	}

	db.wg.Add(1)
	go db.writeHandler()

	for i := 0; i < readerLimit; i++ {
		db.wg.Add(1)
		go db.readHandler()
	}

	return db, nil
}

func (db *Database) writeHandler() {
	defer db.wg.Done()

	for req := range db.writeChan {
		err := db.writeToFile(req.key, req.value)
		req.resp <- err
	}
}

func (db *Database) readHandler() {
	defer db.wg.Done()

	for req := range db.readChan {
		db.mu.RLock()
		pos, ok := db.records[req.key]
		db.mu.RUnlock()

		if !ok {
			req.resp <- readResult{"", ErrKeyMissing}
			continue
		}

		f, err := os.Open(pos.file.Name())
		if err != nil {
			req.resp <- readResult{"", err}
			continue
		}

		e, err := LoadEntry(f, pos.offset)
		f.Close()
		if err != nil {
			req.resp <- readResult{"", err}
			continue
		}

		req.resp <- readResult{e.value, nil}
	}
}

func (db *Database) writeToFile(key, value string) error {
	latest := db.files[len(db.files)-1]
	info, err := latest.Stat()
	if err != nil {
		return err
	}

	offset := info.Size()
	if offset >= maxSize {
		latest, err = db.createNewFile()
		if err != nil {
			return err
		}
		db.files = append(db.files, latest)
		offset = 0
	}

	data := Serialize(kvPair{key, value})
	if _, err := latest.WriteAt(data, offset); err != nil {
		return err
	}

	db.mu.Lock()
	db.records[key] = recordPos{latest, offset}
	db.mu.Unlock()

	return nil
}

func (db *Database) restore(f *os.File) error {
	var offset int64
	for item := range Stream(f) {
		db.records[item.key] = recordPos{f, offset}
		offset += int64(len(item.key) + len(item.value) + 8)
	}
	return nil
}

func (db *Database) Close() error {
	close(db.writeChan)
	close(db.readChan)
	db.wg.Wait()

	for _, f := range db.files {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (db *Database) createNewFile() (*os.File, error) {
	name := baseFilename + strconv.Itoa(len(db.files))
	fullPath := filepath.Join(db.dir, name)
	return os.OpenFile(fullPath, fileFlags, 0o600)
}

func (db *Database) Get(key string) (string, error) {
	resp := make(chan readResult)
	db.readChan <- readRequest{key, resp}
	result := <-resp
	return result.value, result.err
}

func (db *Database) Put(key, value string) error {
	resp := make(chan error)
	db.writeChan <- writeRequest{key, value, resp}
	return <-resp
}

func (db *Database) Size() (int64, error) {
	var total int64
	for _, f := range db.files {
		stat, err := f.Stat()
		if err != nil {
			return 0, err
		}
		total += stat.Size()
	}
	return total, nil
}
