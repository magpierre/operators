package sharedlibrary

import (
	"errors"
	"unsafe"
)

type InternalDataStructure struct {
	Columns int
	Rows    int
	Data    []*Data
}

func (d InternalDataStructure) setPositionValue(col, row int, value any) error {
	if col < 0 || col >= d.Columns {
		return errors.New("column index out of range")
	}
	if row < 0 || row >= d.Rows {
		return errors.New("row index out of range")
	}
	// Set the value at the specified column and row
	(*d.Data[col])[row] = value
	return nil
}

func (d InternalDataStructure) getPositionValue(col, row int) (any, error) {
	if col < 0 || col >= d.Columns {
		return nil, errors.New("column index out of range")
	}
	if row < 0 || row >= d.Rows {
		return nil, errors.New("row index out of range")
	}
	// Get the value at the specified column and row
	return (*d.Data[col])[row], nil
}

func (d *InternalDataStructure) addColumn(data Data) error {
	if len(data) != d.Rows {
		return errors.New("data length does not match")
	}
	// Add the new column at the specified position
	d.Data = append(d.Data, &data)
	d.Columns = len(d.Data)
	return nil
}

func (d *InternalDataStructure) replaceColumn(position int, data Data) error {
	if position < 0 || position >= d.Columns {
		return errors.New("column index out of range")
	}
	if len(data) != d.Rows {
		return errors.New("data length does not match")
	}
	// Add the new column at the specified position
	d.Data[position] = &data
	d.Columns = len(d.Data)
	return nil
}

func (d *InternalDataStructure) dropColumn(position int) error {
	if position < 0 || position >= d.Columns {
		return errors.New("column index out of range")
	}
	// Remove the column at the specified position
	d.Data = append(d.Data[:position], d.Data[position+1:]...)
	d.Columns = len(d.Data)
	return nil
}

func (d InternalDataStructure) getColumn(position int) Data {
	if position < 0 || position >= d.Columns {
		return nil
	}
	// Get the column at the specified position
	return *d.Data[position]
}
func (d *InternalDataStructure) getNumberOfRows() int {
	d.Rows = len(*d.Data[0])
	return d.Rows
}
func (d InternalDataStructure) getNumberOfColumns() int {
	return d.Columns
}

func (d InternalDataStructure) getRow(row int) Row {
	num_cols := len(d.Data)
	new_array := make(Row, num_cols)
	for i := range new_array {
		d1 := d.Data[i]
		startpos := unsafe.Pointer(&(*d1)[0])
		itemSize := unsafe.Sizeof((*d1)[0])
		new_array[i] = (*any)(unsafe.Add(startpos, uintptr(row)*itemSize))
	}
	return new_array
}
