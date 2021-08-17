package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	api "github.com/DpodDani/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	// where 16 is the base offset
	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset,
		fmt.Sprintf("Offset should be: %d", s.nextOffset))
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)
		require.Equal(t, s.nextOffset, 16+i+1)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	// max index
	_, err = s.Append(want)
	require.Equal(t, err, io.EOF)
	require.True(t, s.IsMaxed())

	// max store
	c.Segment.MaxStoreBytes = entWidth * 3
	c.Segment.MaxIndexBytes = 1024
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.True(t, s.IsMaxed())

	err = s.Remove()
	require.NoError(t, err)
	s, err = newSegment(dir, 16, c)
	require.False(t, s.IsMaxed())
}
