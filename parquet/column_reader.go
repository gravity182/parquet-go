package parquet

import "io"

type ColumnReader struct {
	pathInSchema []string

	r      io.ReaderAt
	size   int64
	closer io.Closer

	meta *Metadata
}

type ColumnReaderOptions struct {
	BatchSize int
}

func (r *Reader) NewColumnReader(path ...string) *ColumnReader {
	return &ColumnReader{
		pathInSchema: path,
		r:            r.r,
		size:         r.size,
		closer:       r.closer,
		meta:         r.meta,
	}
}

func (r *ColumnReader) Close() error {
	if r.closer == nil {
		return nil
	}
	return r.closer.Close()
}
