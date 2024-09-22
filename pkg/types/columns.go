/*
These are the types we should start with and expect to work on all
implementations we explore.

It's not a lot, and doesn't cover more complex types like geocoordinate stuff
It also doesn't mean all implementations HAVE to cover all types, as we'll see
it can get complicated FAST otherwise
*/
package types

type ColumnType uint32

const (
	Int32 ColumnType = iota
	Int64
	Uint32
	Uint64
	Float32
	Float64
	String
	Bool
	NestedList
	NestedStruct
)

type Column interface {
	Name() string
	Type() ColumnType
}
