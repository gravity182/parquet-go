package parquet

type Array interface {
	Len() int
	Validity() Validity
}

type Int32Array struct {
	values   []int32
	validity Validity
}

func (a *Int32Array) Len() int {
	return len(a.values)
}

func (a *Int32Array) Validity() Validity {
	return a.validity
}

func (a *Int32Array) Values() []int32 {
	return a.values
}

type Int64Array struct {
	values   []int64
	validity Validity
}

func (a *Int64Array) Len() int {
	return len(a.values)
}

func (a *Int64Array) Validity() Validity {
	return a.validity
}

func (a *Int64Array) Values() []int64 {
	return a.values
}

type BinaryArray struct {
	// continous byte buffer holding all the binary data
	data     []byte
	offsets  []int32
	validity Validity
}

func (a *BinaryArray) Len() int {
	return len(a.offsets) - 1
}

func (a *BinaryArray) Validity() Validity {
	return a.validity
}

func (a *BinaryArray) Data() []byte {
	return a.data
}

func (a *BinaryArray) Offsets() []int32 {
	return a.offsets
}

// String array uses one continous byte buffer to hold string data.
// One must use offsets to read strings
type StringArray struct {
	BinaryArray
}

func (a *StringArray) Len() int {
	return len(a.offsets) - 1
}

func (a *StringArray) Validity() Validity {
	return a.validity
}

func (a *StringArray) Data() []byte {
	return a.data
}

func (a *StringArray) Offsets() []int32 {
	return a.offsets
}

type ListArray struct {
	offsets  []int32
	validity Validity
	values   Array
}

func (a *ListArray) Len() int {
	return len(a.offsets) - 1
}

func (a *ListArray) Validity() Validity {
	return a.validity
}

func (a *ListArray) Values() Array {
	return a.values
}

func (a *ListArray) Offsets() []int32 {
	return a.offsets
}
