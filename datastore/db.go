package datastore

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
)

const (
	baseFilename = "current-data-"
	maxSize      = 10 * 1024 * 1024
	fileFlags    = os.O_RDWR | os.O_CREATE
)

var ErrKeyMissing = errors.New("key not found")

type recordPos struct {
	file   *os.File
	offset int64
}

type Database struct {
	dir     string
	files   []*os.File
	records map[string]recordPos
}

func Open(dir string) (*Database, error) {
	db := &Database{
		dir:     dir,
		files:   []*os.File{},
		records: make(map[string]recordPos),
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
	return db, nil
}

func (db *Database) restore(f *os.File) error {
	var offset int64
	for item := range Stream(f) {
		db.records[item.key] = recordPos{f, offset}
		offset += int64(len(item.key)+len(item.value) + 8)
	}
	return nil
}

func (db *Database) Close() error {
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
	pos, ok := db.records[key]
	if !ok {
		return "", ErrKeyMissing
	}
	e, err := LoadEntry(pos.file, pos.offset)
	if err != nil {
		return "", err
	}
	return e.value, nil
}

func (db *Database) Put(key, value string) error {
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
	db.records[key] = recordPos{latest, offset}
	return nil
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
