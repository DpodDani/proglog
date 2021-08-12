package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	// creating the store again to verify that previous writes are preserved
	// i.e. in the case of the log service recovering from failure
	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth) // record's length value
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)
		off += int64(n)

		// enc.Uint64 --> converts byte sequence into 64-bit unsigned integer
		size := enc.Uint64(b)  // obtain the length of the record data
		b = make([]byte, size) // create byte array to hold record data
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, write, b)
		// size --> integer value representing length of record data
		// n --> number of bytes read
		// each character in record data is an ASCII character,
		// therefore one byte = one character,
		// therefore this comparison can be made
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	_, _, err = s.Append(write)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
	require.Equal(t, afterSize, int64(width))
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644, // 6 = R+W, 4 = R
	)

	if err != nil {
		return nil, 0, err
	}

	fi, err := f.Stat() // FileInfo struct describing file
	if err != nil {
		return nil, 0, err
	}

	return f, fi.Size(), nil
}
