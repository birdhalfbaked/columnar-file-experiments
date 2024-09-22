/*
Naive files will just be a single-pass write solution with the following format
spec
+------------------------------------------------------+
|                                                      |
|                         DATA                         |
|                                                      |
+------------------------------------------------------+
|                       Metadata                       |
| Row count (uint64)                                   |
| Column count (uint64)                                |
| Columns [64 byte name, 4 byte type, 8 byte offset]   |
| Metadata length uint32                               |
+------------------------------------------------------+

This is not efficiently packed, nor is it optimized for compression,
etc. but it gets the job done.

It also has one massive limitation: can't really do nested structures.
Well... technically you can, but nested structure striping is not imo a naive solution.
For now we can just do the silly thing.
*/
package naive

import (
	"columnarfiles/pkg/types"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
)

type NaiveColumn struct {
	name   string
	_type  types.ColumnType
	offset uint64
}

func (n NaiveColumn) Name() string {
	return n.name
}
func (n NaiveColumn) Type() types.ColumnType {
	return types.ColumnType(n._type)
}

func NewColumn(name string, _type types.ColumnType) NaiveColumn {
	return NaiveColumn{
		name:  name,
		_type: _type,
	}
}

func (n *NaiveColumn) Deserialize(data []byte) any {
	switch n._type {
	case types.Int32:
		var v int32
		binary.Decode(data, binary.LittleEndian, &v)
		return v
	case types.Int64:
		var v int64
		binary.Decode(data, binary.LittleEndian, &v)
		return v
	case types.Uint32:
		var v uint32
		binary.Decode(data, binary.LittleEndian, &v)
		return v
	case types.Uint64:
		var v uint64
		binary.Decode(data, binary.LittleEndian, &v)
		return v
	case types.Float32:
		var v float32
		binary.Decode(data, binary.LittleEndian, &v)
		return v
	case types.Float64:
		var v float64
		binary.Decode(data, binary.LittleEndian, &v)
		return v
	case types.String:
		return string(data)
	case types.Bool:
		var v bool = data[0] != 0x00
		return v
	default:
		panic(fmt.Sprintf("unexpected naive.NaiveColumnType: %#v", n._type))
	}
}

func (n *NaiveColumn) Serialize(value interface{}) ([]byte, error) {
	switch n._type {
	case types.Int32:
		var out = make([]byte, 4)
		_, err := binary.Encode(out, binary.LittleEndian, value.(int32))
		return out, err
	case types.Int64:
		var out = make([]byte, 8)
		_, err := binary.Encode(out, binary.LittleEndian, value.(int64))
		return out, err
	case types.Uint32:
		var out = make([]byte, 4)
		_, err := binary.Encode(out, binary.LittleEndian, value.(uint32))
		return out, err
	case types.Uint64:
		var out = make([]byte, 8)
		_, err := binary.Encode(out, binary.LittleEndian, value.(uint64))
		return out, err
	case types.Float32:
		var out = make([]byte, 4)
		_, err := binary.Encode(out, binary.LittleEndian, value.(float32))
		return out, err
	case types.Float64:
		var out = make([]byte, 8)
		_, err := binary.Encode(out, binary.LittleEndian, value.(float64))
		return out, err
	case types.String:
		str := value.(string)
		if len(str) > 1<<16 {
			str = str[:1<<16]
		}
		// for tighter packing, add length info
		lengthBuf := make([]byte, 2)
		binary.LittleEndian.PutUint16(lengthBuf, uint16(len(str)))
		return append(lengthBuf, []byte(str)...), nil
	case types.Bool:
		var out = []byte{0x00}
		if value.(bool) {
			out[0] = 0x01
		}
		return out, nil
	default:
		panic(fmt.Sprintf("unexpected naive.NaiveColumnType: %#v", n._type))
	}

}

type NaiveColumnData struct {
	ColumnDef *NaiveColumn
	Data      []byte
}

type NaiveColumnarMetadata struct {
	RowCount    uint64
	ColumnCount uint64
	Columns     []*NaiveColumn
}

func (m *NaiveColumnarMetadata) Serialize() ([]byte, error) {
	buff := make([]byte, 16)
	_, err := binary.Encode(buff, binary.LittleEndian, [2]uint64{m.RowCount, m.ColumnCount})
	if err != nil {
		return nil, err
	}
	for i := 0; i < int(m.ColumnCount); i++ {
		col := m.Columns[i]
		bytes := []byte(col.name)
		if len(bytes) > 64 {
			bytes = bytes[:64]
		} else {
			bytes = append(bytes, make([]byte, 64-len(bytes))...)
		}
		buff = append(buff, bytes...)                                    // 64 byte padded name
		buff = binary.LittleEndian.AppendUint32(buff, uint32(col._type)) // Type
		buff = binary.LittleEndian.AppendUint64(buff, col.offset)        // Type
	}
	buff = binary.LittleEndian.AppendUint32(buff, uint32(len(buff)))
	return buff, nil
}

func DeserializeMetadata(data []byte) (*NaiveColumnarMetadata, error) {
	metadata := &NaiveColumnarMetadata{}
	metadata.RowCount = binary.LittleEndian.Uint64(data[:8])
	metadata.ColumnCount = binary.LittleEndian.Uint64(data[8:16])
	metadata.Columns = make([]*NaiveColumn, metadata.ColumnCount)
	for i := 0; i < int(metadata.ColumnCount); i++ {
		offset := 16 + i*76
		metadata.Columns[i] = &NaiveColumn{
			name:   string(data[offset : offset+64]),
			_type:  types.ColumnType(binary.LittleEndian.Uint32(data[offset+64 : offset+68])),
			offset: binary.LittleEndian.Uint64(data[offset+68 : offset+76]),
		}
	}

	return metadata, nil
}

type NaiveColumnarFile struct {
	File     *os.File
	Metadata *NaiveColumnarMetadata
	// use this for slight optimization of writes
	dirty bool
}

func (n *NaiveColumnarFile) Close() error {
	return n.File.Close()
}

// Open opens a file that is a NaiveColumnarFile.
// for convenience this will create if it doesn't exist
func Open(path string) (*NaiveColumnarFile, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}
	nFile := &NaiveColumnarFile{
		File:     file,
		Metadata: nil,
		dirty:    false,
	}
	// Grab the metadata size
	file.Seek(-4, 2)
	metadataSizeBuf := make([]byte, 4)
	_, err = file.Read(metadataSizeBuf)
	if err == io.EOF {
		// Empty file, just return
		return nFile, nil
	} else if err != nil {
		slog.Error(err.Error())
		return nil, err
	}
	metadataSize := binary.LittleEndian.Uint32(metadataSizeBuf) + 4
	// Seek to the beginning of the metadata and start reading
	file.Seek(-int64(metadataSize), 2)
	metadataBuf := make([]byte, metadataSize)
	file.Read(metadataBuf)
	// Attempt to serialize
	metadata, err := DeserializeMetadata(metadataBuf)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}
	file.Seek(0, 0)
	nFile.Metadata = metadata
	return nFile, nil
}

func reflectColumnsFromData(v any) []*NaiveColumn {
	columns := make([]*NaiveColumn, 0)
	reflectedType := reflect.TypeOf(v)
	for i := 0; i < reflectedType.NumField(); i++ {
		field := reflectedType.Field(i)
		switch k := field.Type.Kind(); k {
		case reflect.Uint32:
			columns = append(columns, &NaiveColumn{name: field.Name, _type: types.Uint32})
		case reflect.Uint64:
			columns = append(columns, &NaiveColumn{name: field.Name, _type: types.Uint64})
		case reflect.Int32:
			columns = append(columns, &NaiveColumn{name: field.Name, _type: types.Int32})
		case reflect.Int64:
			columns = append(columns, &NaiveColumn{name: field.Name, _type: types.Int64})
		case reflect.Float32:
			columns = append(columns, &NaiveColumn{name: field.Name, _type: types.Float32})
		case reflect.Float64:
			columns = append(columns, &NaiveColumn{name: field.Name, _type: types.Float64})
		case reflect.String:
			columns = append(columns, &NaiveColumn{name: field.Name, _type: types.String})
		case reflect.Bool:
			columns = append(columns, &NaiveColumn{name: field.Name, _type: types.Bool})
		default:
			panic(fmt.Sprintf("unexpected kind: %v", k))
		}
	}
	fmt.Println(columns[1])
	return columns
}

// Write takes the schema defined as a list of column definitions and then
// a list of data objects, which are then serialized using the information
// from the column info relating to that column index
func (n *NaiveColumnarFile) Write(values []any) {
	data := values
	columns := reflectColumnsFromData(data[0])
	numCols := len(columns)
	columnDataArr := make([]*NaiveColumnData, numCols)
	for i := 0; i < numCols; i++ {
		columnDataArr[i] = &NaiveColumnData{
			ColumnDef: columns[i],
			Data:      make([]byte, 0),
		}
	}
	// okay so this is where this really will fall short, because we need to do the
	// too-much-in-memory disco
	byteOffset := 0 // can't forget that we must store this for efficient jump-reads
	for colIndex := 0; colIndex < numCols; colIndex++ {
		var colObj = columnDataArr[colIndex]
		colObj.ColumnDef.offset = uint64(byteOffset)
		for i := 0; i < len(data); i++ {
			val := reflect.ValueOf(data[i]).Field(colIndex).Interface()
			bytes, err := colObj.ColumnDef.Serialize(val)
			if err != nil {
				panic(err.Error())
			}
			byteOffset += len(bytes)
			colObj.Data = append(colObj.Data, bytes...)
		}
	}

	n.dirty = true
	// okay that sucked, but hey we're here now and can write to the file
	for colIndex := 0; colIndex < numCols; colIndex++ {
		n.File.Write(columnDataArr[colIndex].Data)
	}
	// and now we write the metadata
	n.Metadata = &NaiveColumnarMetadata{
		RowCount:    uint64(len(data)),
		ColumnCount: uint64(len(columns)),
		Columns:     columns,
	}
	metadataBytes, err := n.Metadata.Serialize()
	if err != nil {
		slog.Error(err.Error())
	}
	n.File.Write(metadataBytes)
	n.File.Sync()
}

func (n *NaiveColumnarFile) Scan(columns ...string) [][]any {
	results := make([][]any, len(columns))
	for i := 0; i < len(columns); i++ {
		results = append(results, make([]any, 0))
	}

	for i, colId := range columns {
		var col *NaiveColumn
		for j := range n.Metadata.Columns {
			col = n.Metadata.Columns[j]
			if col.name == colId {
				n.File.Seek(int64(col.offset), 0)
				break
			}
		}
		var valBuf []byte
		var lenBuf []byte = make([]byte, 2) // only needed for strings
		switch col._type {
		case types.Uint32, types.Int32, types.Float32:
			valBuf = make([]byte, 4)
		case types.Uint64, types.Int64, types.Float64:
			valBuf = make([]byte, 8)
		case types.Bool:
			valBuf = make([]byte, 1)
		}
		// read pass
		for rowNum := uint64(0); rowNum < n.Metadata.RowCount; rowNum++ {
			if col._type == types.String {
				n.File.Read(lenBuf)
				var length = binary.LittleEndian.Uint16(lenBuf)
				valBuf = make([]byte, length)
			}
			n.File.Read(valBuf)
			results[i] = append(results[i], col.Deserialize(valBuf))
		}
	}
	return results
}
