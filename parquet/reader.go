package parquet

import (
	"fmt"
	"io"
	"os"
)

type Reader struct {
	r      io.ReaderAt
	size   int64
	closer io.Closer
}

func NewReader(r io.ReaderAt, size int64, closer io.Closer) *Reader {
	return &Reader{
		r:      r,
		size:   size,
		closer: closer,
	}
}

func OpenFile(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open a Parquet file '%q': %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat a Parquet file '%q': %w", path, err)
	}
	return NewReader(f, info.Size(), f), nil
}

func (r *Reader) Close() error {
	if r.closer == nil {
		return nil
	}
	if err := r.closer.Close(); err != nil {
		return fmt.Errorf("close reader: %w", err)
	}
	return nil
}
