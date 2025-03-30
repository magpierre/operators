package sharedlibrary

type DataFrameInterface interface {
	// Getters
	GetColumn(colname string) Data
	GetColumnByIndex(index int) Data
	GetFieldNameByIndex(index int) string
	GetFieldNames() []string
	GetFieldNumber(fieldname string) int
	GetFieldType(fieldname string) string
	GetFieldTypes() []string
	GetNumberOfColumns() int
	GetNumberOfRows() int
	GetPositionValue(col, row int) (any, error)
	GetRowByIndex(index int) Row
	GetSchema() *Schema

	// Setters
	SetPositionValue(col, row int, value any) error

	// Other operations
	AddColumn(fieldname string, data Data) error
	AddFunction(name string, f interface{})
	Count() int
	Describe() map[string]interface{}
	Distinct(fieldname string) []string
	DropColumn(fieldname string) error
	GenerateStats()
	IndexRows() error
	Project(fields ...string) (*DataFrame, error)
	RenameColumn(old_fieldname, new_fieldname string) error
	RemoveFunction(name string)
	Transform(value string) error
	UnionAll(otherDF *DataFrame) (*DataFrame, error)
	Where(value string) (*DataFrame, error)
}

// Ensure the file ends with a newline

type DataStructure interface {
	// Getters
	getColumn(position int) Data
	getNumberOfColumns() int
	getNumberOfRows() int
	getPositionValue(col, row int) (any, error)
	getRow(row int) Row

	// Setters
	setPositionValue(col, row int, value any) error

	// Other operations
	addColumn(data Data) error
	dropColumn(position int) error
	replaceColumn(position int, data Data) error
}

// types

type metadata struct {
	Source   string
	Rows     int
	Columns  int
	RowStats map[string]RowStat
}

type Data = []any
type Row = []*any
type RowStat = map[string]interface{}
