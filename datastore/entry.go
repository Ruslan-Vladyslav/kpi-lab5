package datastore

import (
	"encoding/binary"
	"io"
	"iter"
	"os"
)

type kvPair struct {
	key   string
	value string
}

func Serialize(pair kvPair) []byte {
	kLen := len(pair.key)
	vLen := len(pair.value)
	buf := make([]byte, kLen+vLen+8)

	binary.LittleEndian.PutUint32(buf[0:4], uint32(kLen))
	copy(buf[4:], pair.key)
	binary.LittleEndian.PutUint32(buf[4+kLen:8+kLen], uint32(vLen))
	copy(buf[8+kLen:], pair.value)

	return buf
}

func readString(r io.ReaderAt, offset int64) (string, int, error) {
	header := make([]byte, 4)
	if _, err := r.ReadAt(header, offset); err != nil {
		return "", 0, err
	}
	length := int(binary.LittleEndian.Uint32(header))
	content := make([]byte, length)
	if _, err := r.ReadAt(content, offset+4); err != nil {
		return "", 0, err
	}
	return string(content), 4 + length, nil
}

func LoadEntry(r io.ReaderAt, offset int64) (kvPair, error) {
	k, kSize, err := readString(r, offset)
	if err != nil {
		return kvPair{}, err
	}
	v, _, err := readString(r, offset+int64(kSize))
	if err != nil {
		return kvPair{}, err
	}
	return kvPair{k, v}, nil
}

func Stream(f *os.File) iter.Seq[kvPair] {
	return func(yield func(kvPair) bool) {
		var offset int64
		for {
			pair, err := LoadEntry(f, offset)
			if err != nil {
				return
			}
			offset += int64(len(pair.key)+len(pair.value) + 8)
			if !yield(pair) {
				return
			}
		}
	}
}
