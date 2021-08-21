package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/DpodDani/proglog/api/v1"
)

type Log struct {
	mu sync.RWMutex

	Dir    string
	Config Config

	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1014
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

func (l *Log) setup() error {
	// fetch all the index and store files
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, file := range files {
		// obtain offset (string) number from fetched index and store files
		// note: we'll have duplicate offset numbers - one from each pair of
		// index and store files
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		// converts offStr (decimal base) into an uint64 value
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	// sort array of offsets in ascending order
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// TODO: why not use i+=2 instead?
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffset contains duplicate for index and store,
		// so we skip the duplicate value
		i++
	}

	if l.segments == nil {
		if err = l.newSegment(
			l.Config.Segment.InitialOffset, // zero value is 0
		); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1) // TODO: revisit this line!
	}

	return off, err
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var s *segment
	for _, segment := range l.segments {
		if off >= segment.baseOffset && off < segment.nextOffset {
			s = segment
			break
		}
	}
	if s == nil || off >= s.nextOffset {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}
	return s.Read(off)
}

// closes log's segments
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// closes log and removes its data
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// removes log and creates a new log to replace it
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

// remove segments with offsets lower than or equal to "lowest" (input argument)
// the idea is to remove old segments (whose data we would have already
// processed)
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

// this struct is used to ensure two things:
// 1) to satisfy the io.Reader interface (by implementing the Read() function)
// 2) to ensure we begin reading store, for each segment, from the beginning,
// and therefore read its entire file
type originReader struct {
	*store
	off int64 // zero value is 0
}

func (o *originReader) Read(p []byte) (int, error) {
	// using ReadAt function from the store struct
	// store is a "promoted field" in the originReader struct,
	// therefore the ReadAt() function call "passes through" to the
	// store (the promoted anonymous field)
	// ref: https://stackoverflow.com/a/58119914
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

// returns an io.Reader for reading the whole log
// this functionality will be used later to support snapshots and restoring
// a log
func (l *Log) Reader() io.Reader {
	l.mu.Lock()
	defer l.mu.Unlock()

	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	// io.MultiReader concanates segments' store
	return io.MultiReader(readers...)
}
