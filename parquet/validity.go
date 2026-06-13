package parquet

type Validity interface {
	IsValid(idx int) bool
}

type BitmapValidity struct {
	valid []uint64
}

func (v *BitmapValidity) isValid(idx int) bool {
	bitOffset := idx
	intIndex := bitOffset / 64
	bitIndex := bitOffset % 64

	bit := (v.valid[intIndex] >> bitIndex) & 1
	return bit == 1
}

type AllValidity struct {
}

func (v *AllValidity) isValid(idx int) bool {
	return true
}

type NoValidity struct {
}

func (v *NoValidity) isValid(idx int) bool {
	return false
}
