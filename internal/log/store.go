package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian // encoding used for record size and index entries
)

const (
	lenWidth = 8 // number of bytes used to store record's length
)

type store struct {
	*os.File // anonymous field
	mu       sync.Mutex
	buf      *bufio.Writer
	size     uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// persists given bytes to the store
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// assiging value to one of the return values
	// pos -> position where the store holds the record in its file
	pos = s.size
	// write length of record to file
	// so when we read record, we know how many bytes to read
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// w -> number of bytes written to buffer
	// we write to buffer (instead of directly to file) to improve performance
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth // take into account writing record length to file!
	s.size += uint64(w)
	return uint64(w), pos, nil
}
