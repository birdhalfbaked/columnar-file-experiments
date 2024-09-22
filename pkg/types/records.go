package types

import (
	"errors"
)

type Record struct {
	Data []any
}

func (r *Record) Scan(dest ...any) error {
	if len(r.Data) != len(dest) {
		return errors.New("invalid number of destination variables passed")
	}
	for i, val := range r.Data {
		switch d := dest[i].(type) {
		case *int32:
			*d = val.(int32)
		case *int64:
			*d = val.(int64)
		case *uint32:
			*d = val.(uint32)
		case *uint64:
			*d = val.(uint64)
		case *float32:
			*d = val.(float32)
		case *float64:
			*d = val.(float64)
		case *string:
			*d = val.(string)
		case *bool:
			*d = val.(bool)
		}
	}
	return nil
}
