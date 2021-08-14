package log

import (
	"io"
	"os"

	"github.com/tysontate/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func NewIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())
	// Truncate() either removes excess data from the file,
	// or fills it with null characters so that file size matches
	// value in 2nd argument.
	//
	// This is because the file cannot grow after it has been memory-mapped,
	// so better to do it at the beginning.
	//
	// This means the last index entry is not necessarily at the end of the file
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := i.file.Sync(); err != nil {
		return err
	}

	// Truncate() is called again to get rid of any excess null characters,
	// which were potentially introduced by Truncate() in index constructor.
	//
	// This enables the service to find the last index entry and determine
	// the next offset.
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

// in --> offset (relative to Segment's base offset)
// 0 = offset of index's first entry; 1 = second entry etc.
// Relative offset values allow us to store as uint32
// Absolute offset values would have to be stored as uint64,
// thereby taking up 4 more bytes for each entry,
// because 32 extra bits = 4 extra bytes
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	// When in == -1, we want to return the last entry
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	// validate that there's enough space in index to add a new entry
	if i.size+entWidth > uint64(len(i.mmap)) {
		return io.EOF
	}

	// encode offset and position, and write to memory-mapped file
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += entWidth // increment position where next write will go
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
