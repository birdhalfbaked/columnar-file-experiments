package columnarfiles

type DataReader interface {
	Read(...any) error
}

type ColumnarStore interface {
	// Scan takes some lowerlevel filters and returns records
	Scan(columns ...string) [][]any
	// Write is similar to Append, but does an overwrite based only on the records passed.
	Write([]any)
	// Closes the file and does any flushing, etc.
	Close() error
}
