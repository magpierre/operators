package sharedlibrary

import (
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
)

type DataFrame struct {
	Schema    Schema
	Data      DataStructure //[]*Data
	row       map[int]Row
	metadata  metadata
	functions map[string]interface{}
}

func NewDataFrame() *DataFrame {
	return &DataFrame{
		Schema: Schema{
			Fields: make([]Field, 0),
		},
		Data: &InternalDataStructure{
			Data:    make([]*Data, 0),
			Rows:    0,
			Columns: 0,
		},
	}
}
func NewDataFrameWithArgs(Fields []Field, data []*Data) *DataFrame {
	return &DataFrame{
		Schema: Schema{
			Fields: Fields,
		},
		Data: &InternalDataStructure{
			Data:    data,
			Rows:    len(*data[0]),
			Columns: len(data),
		},
	}
}

func (d *DataFrame) GetSchema() *Schema {
	return &d.Schema
}

// GetFieldNumber returns the position of a field in the schema by its name.
func (d *DataFrame) GetFieldNumber(fieldname string) int {
	return d.Schema.GetField(fieldname)
}

func (d DataFrame) GetPositionValue(col, row int) (any, error) {
	return d.Data.getPositionValue(col, row)
}

func (d *DataFrame) RenameColumn(old_fieldname, new_fieldname string) error {
	x := d.Schema.GetField(old_fieldname)
	if x < 0 {
		return errors.New("field not found")
	}
	d.Schema.Fields[x].FieldName = new_fieldname
	return nil
}

// AddColumn adds a new column to the DataFrame.
func (d *DataFrame) AddColumn(fieldname string, data Data) error {
	if len(data) != d.Data.getNumberOfRows() {
		return errors.New("data length does not match")
	}
	d.Schema.Fields = append(d.Schema.Fields, Field{FieldName: fieldname, FieldPosition: len(d.Schema.Fields), FieldType: "string"})
	d.Data.addColumn(data)
	return nil
}

// DropColumn removes a column from the DataFrame.
func (d *DataFrame) DropColumn(fieldname string) error {
	x := d.Schema.GetField(fieldname)
	if x < 0 {
		return errors.New("field not found")
	}
	d.Schema.Fields = append(d.Schema.Fields[:x], d.Schema.Fields[x+1:]...)
	d.Data.dropColumn(x)
	return nil
}

// GetFieldNames returns the names of all fields in the schema.
func (d *DataFrame) GetFieldNames() []string {
	names := make([]string, 0)
	for _, v := range d.Schema.Fields {
		names = append(names, v.FieldName)
	}
	return names
}

// GetFieldTypes returns the types of all fields in the schema.
func (d *DataFrame) GetFieldTypes() []string {
	types := make([]string, 0)
	for _, v := range d.Schema.Fields {
		types = append(types, v.FieldType)
	}
	return types
}

func (d DataFrame) GetNumberOfColumns() int {
	return len(d.Schema.Fields)
}

func (d DataFrame) GetNumberOfRows() int {
	return d.Data.getNumberOfRows()
}

// Get statistics for a DataFrame
func (d *DataFrame) Describe() map[string]interface{} {
	stats := make(map[string]interface{})
	for _, v := range d.Schema.Fields {
		x := d.GetFieldNumber(v.FieldName)
		field := d.Data.getColumn(x)
		stats[v.FieldName] = map[string]interface{}{
			"count": len(field),
			"mean":  0,
			"min":   0,
			"max":   0,
		}
	}
	return stats
}

// GenerateStats generates statistics for a DataFrame.
func (d *DataFrame) GenerateStats() {
	for _, v := range d.Schema.Fields {
		x := d.GetFieldNumber(v.FieldName)
		field := d.Data.getColumn(x)
		stats := make(map[string]interface{})
		rlen := len(field)
		d.metadata.Columns = rlen
		d.metadata.RowStats = make(map[string]RowStat)
		d.metadata.RowStats[v.FieldName] = stats
	}
}

// GetFieldType returns the type of a field in the schema by its name.
func (d *DataFrame) GetFieldType(fieldname string) string {
	return *d.Schema.GetType(fieldname)
}

// Project creates a new DataFrame containing only the specified fields.
// Returns an error if any of the fields are not found in the schema.
func (d *DataFrame) Project(fields ...string) (*DataFrame, error) {
	_fields := make([]Field, 0)
	_data := make([]*Data, 0)

	for i, v := range fields {
		x := d.GetSchema().GetField(v)
		if x < 0 {

			s := fmt.Sprintf("Field, not found in Schema:%v", *d.GetSchema())
			return nil, errors.New(s)
		}
		_fields = append(_fields, d.Schema.Fields[x])
		_fields[len(_fields)-1].FieldPosition = i
		j := d.Data.getColumn(x)
		_data = append(_data, &j)
	}
	return &DataFrame{
		Schema: Schema{
			Fields: _fields,
		},
		Data: &InternalDataStructure{
			Data:    _data,
			Rows:    len(*_data[0]),
			Columns: len(_data),
		},
	}, nil
}

// UnionAll combines the current DataFrame with another DataFrame.
// Returns an error if the schemas of the two DataFrames do not match.
func (d *DataFrame) UnionAll(otherDF *DataFrame) (*DataFrame, error) {

	other_df_field_len := len(otherDF.Schema.Fields)
	current_df_field_len := len(d.Schema.Fields)
	if other_df_field_len != current_df_field_len {
		return nil, errors.New("columns does not match")
	}

	for i, v := range d.Schema.Fields {
		other_f := otherDF.Schema.Fields[i]
		if v.FieldName != other_f.FieldName {
			return nil, errors.New("column Names does not match")
		}
		if v.FieldType != other_f.FieldType {
			return nil, errors.New("column types does not match")
		}
	}
	// Ensure d.Data and otherDF.Data are cast to InternalDataStructure
	dInternal, ok := d.Data.(*InternalDataStructure)
	if !ok {
		return nil, errors.New("d.Data is not of type *InternalDataStructure")
	}
	otherInternal, ok := otherDF.Data.(*InternalDataStructure)
	if !ok {
		return nil, errors.New("otherDF.Data is not of type *InternalDataStructure")
	}

	// Combine data from both DataFrames
	ds2 := make([]*Data, len(dInternal.Data))
	copy(ds2, dInternal.Data)
	for i := range ds2 {
		x := make([]any, 0)
		x = append(x, (*ds2[i])...)
		x = append(x, (*otherInternal.Data[i])...)
		ds2[i] = &x
	}

	// Return a new DataFrame with the combined data
	return &DataFrame{
		Schema: d.Schema,
		Data: &InternalDataStructure{
			Data:    ds2,
			Rows:    dInternal.Rows + otherInternal.Rows,
			Columns: dInternal.Columns,
		},
	}, nil
}

// Join performs an inner join between two DataFrames based on the specified keys.
// Returns a new DataFrame containing the joined data.
func (d *DataFrame) Join(otherDF *DataFrame, keys []string) (*DataFrame, error) {
	// Validate keys
	if len(keys) == 0 {
		return nil, errors.New("no keys provided for join")
	}

	// Map to store the index of keys in both DataFrames
	keyIndices := make([][2]int, len(keys))
	for i, key := range keys {
		leftIndex := d.GetFieldNumber(key)
		rightIndex := otherDF.GetFieldNumber(key)
		if leftIndex < 0 || rightIndex < 0 {
			return nil, fmt.Errorf("key '%s' not found in one of the DataFrames", key)
		}
		keyIndices[i] = [2]int{leftIndex, rightIndex}
	}

	// Create a map for the right DataFrame to index rows by key values
	rightIndexMap := make(map[string][]Row)
	for i := 0; i < otherDF.GetNumberOfRows(); i++ {
		row := otherDF.getRow(i)
		keyValues := make([]string, len(keys))
		for j, indices := range keyIndices {
			keyValues[j] = fmt.Sprintf("%v", (*row[indices[1]]))
		}
		compoundKey := strings.Join(keyValues, "|")
		rightIndexMap[compoundKey] = append(rightIndexMap[compoundKey], row)
	}

	// Prepare schema and data for the resulting DataFrame
	newFields := append(d.Schema.Fields, otherDF.Schema.Fields...)
	newData := make([]*Data, len(newFields))
	for i := range newData {
		newData[i] = &Data{}
	}

	// Perform the join
	for i := 0; i < d.GetNumberOfRows(); i++ {
		leftRow := d.getRow(i)
		keyValues := make([]string, len(keys))
		for j, indices := range keyIndices {
			keyValues[j] = fmt.Sprintf("%v", (*leftRow[indices[0]]))
		}
		compoundKey := strings.Join(keyValues, "|")

		if matchingRows, found := rightIndexMap[compoundKey]; found {
			for _, rightRow := range matchingRows {
				// Combine rows from both DataFrames
				combinedRow := append(leftRow, rightRow...)
				for j, value := range combinedRow {
					(*newData[j]) = append((*newData[j]), *value)
				}
			}
		}
	}

	// Return the new DataFrame
	return &DataFrame{
		Schema: Schema{
			Fields: newFields,
		},
		Data: &InternalDataStructure{
			Data:    newData,
			Rows:    len(*newData[0]),
			Columns: len(newData),
		},
	}, nil
}

// getRow retrieves a row from the DataFrame at the specified position.
// Uses unsafe operations to access the data directly.
func (d *DataFrame) getRow(position int) Row {
	return d.Data.getRow(position)
}

// IndexRows indexes all rows in the DataFrame for quick access.
func (d *DataFrame) IndexRows() error {
	x := d.Data.getColumn(0)
	if x == nil {
		return errors.New("no data found")
	}
	data_length := d.Data.getNumberOfRows()
	if data_length == 0 {
		return errors.New("no data found")
	}

	d.row = make(map[int]Row)
	for i := 0; i < data_length; i++ {
		r := d.getRow(i)
		d.row[i] = r
	}
	return nil
}

// GetColumn retrieves a column from the DataFrame by its name.
func (d *DataFrame) GetColumn(colname string) Data {
	x := d.GetFieldNumber(colname)
	return d.Data.getColumn(x)
}

func (d *DataFrame) GetColumnByIndex(index int) Data {
	return d.Data.getColumn(index)
}

func (d *DataFrame) GetRowByIndex(index int) Row {
	return d.row[index]
}

// GetFieldNameByIndex retrieves the field name from the DataFrame's schema
// based on the provided index. The index corresponds to the position of the
// field in the schema's Fields slice.
//
// Parameters:
//   - index: The zero-based position of the field in the schema.
//
// Returns:
//   - A string representing the name of the field at the specified index.
//
// Note:
//   - Ensure the index is within the bounds of the Fields slice to avoid
//     runtime panics.
func (d *DataFrame) GetFieldNameByIndex(index int) string {
	return d.Schema.Fields[index].FieldName
}

// AddFunction adds a custom function to the DataFrame's environment.
func (d *DataFrame) AddFunction(name string, f interface{}) {
	d.functions[name] = f
}

// RemoveFunction removes a custom function from the DataFrame's environment.
func (d *DataFrame) RemoveFunction(name string) {
	delete(d.functions, name)
}

// addInitialFunctions initializes the default functions for the DataFrame.
func (d *DataFrame) addInitialFunctions() {
	d.functions = make(map[string]interface{})
	/*d.functions["contains"] = func(s, substr string) bool {
		return strings.Contains(s, substr)
	}
	d.functions["startswith"] = func(s, prefix string) bool {
		return strings.HasPrefix(s, prefix)
	}
	d.functions["endswith"] = func(s, suffix string) bool {
		return strings.HasSuffix(s, suffix)
	}
	d.functions["regex"] = func(s, pattern string) (bool, error) {
		return regexp.MatchString(pattern, s)
	}
	d.functions["len"] = func(s string) int {
		return len(s)
	}
	d.functions["tolower"] = func(s string) string {
		return strings.ToLower(s)
	}
	d.functions["toupper"] = func(s string) string {
		return strings.ToUpper(s)
	}
	d.functions["trim"] = func(s string) string {
		return strings.TrimSpace(s)
	}
	d.functions["replace"] = func(s, old, new string) string {
		return strings.ReplaceAll(s, old, new)
	}
	d.functions["strsplit"] = func(s, sep string) []string {
		return strings.Split(s, sep)
	}
	d.functions["strjoin"] = func(parts []string, sep string) string {
		return strings.Join(parts, sep)
	}
	d.functions["concat"] = func(a, b string) string {
		return a + b
	}
	*/
}

type Visitor struct {
	Identifiers []string
}

func (v *Visitor) Visit(node *ast.Node) {
	if n, ok := (*node).(*ast.IdentifierNode); ok {
		v.Identifiers = append(v.Identifiers, n.Value)
	}
}

func (d *DataFrame) Transform(value string) error {
	d.addInitialFunctions()
	// Define the environment for the expression
	num_fields := len(d.Schema.Fields)
	env := map[string]interface{}{
		"functions": d.functions,
	}

	for x := 0; x < num_fields; x++ {
		env[d.GetFieldNameByIndex(x)] = d.GetColumnByIndex(x)
	}
	// Compile the expression
	program, err := expr.Compile(value, expr.Env(env))
	if err != nil {
		log.Fatal(err)
	}
	// Evaluate the expression
	result, err := expr.Run(program, env)
	if err != nil {
		log.Fatal(err)
	}
	node := program.Node()
	v := &Visitor{}
	ast.Walk(&node, v)
	idx := d.GetFieldNumber(v.Identifiers[len(v.Identifiers)-1])
	resultData, ok := result.([]any)
	if !ok {
		new_data := make(Data, d.Data.getNumberOfRows())
		for i := range new_data {
			new_data[i] = result
		}
		resultData = new_data
	}
	if idx < 0 {
		d.AddColumn(v.Identifiers[len(v.Identifiers)-1], resultData)
	} else {
		d.Data.replaceColumn(idx, resultData)
	}

	return nil
}

// Where filters the DataFrame based on a condition applied to a specific field.
// Returns a new DataFrame containing only the rows that satisfy the condition.
func (d *DataFrame) Where(value string) (*DataFrame, error) {
	d.addInitialFunctions()
	// Define the environment for the expression
	num_fields := len(d.Schema.Fields)
	num_rows := d.GetNumberOfRows()
	rows := make(map[int]bool, 0)
	env := map[string]any{
		"functions": d.functions,
	}
	// Compile the expression
	r := d.Data.getRow(0)
	for x := 0; x < num_fields; x++ {
		env[d.GetFieldNameByIndex(x)] = *r[x]
	}
	program, err := expr.Compile(value, expr.Env(env))
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < num_rows; i++ {
		env := map[string]interface{}{
			"functions": d.functions,
		}
		r = d.Data.getRow(i)
		for x := 0; x < num_fields; x++ {
			env[d.GetFieldNameByIndex(x)] = *r[x]
		}
		// Evaluate the expression
		result, err := expr.Run(program, env)
		if err != nil {
			return nil, err
		}
		if _, ok := result.(bool); !ok {
			return nil, errors.New("condition must return a boolean value")
		}
		// Check if the result is true
		if result.(bool) {
			rows[i] = true

		}
	}
	num_rows = len(rows)
	new_data_structure := make([]*Data, num_fields)
	for i := 0; i < num_fields; i++ {
		_d := make(Data, num_rows)
		new_data_structure[i] = &_d
	}
	// Get all the keys and sort them
	sorted_rows := slices.Sorted(maps.Keys(rows))
	for i, v := range sorted_rows {
		r := d.Data.getRow(v)
		for j, v := range new_data_structure {
			(*v)[i] = *r[j]
		}

	}
	return &DataFrame{
		Schema: d.Schema,
		Data: &InternalDataStructure{
			Data:    new_data_structure,
			Rows:    num_rows,
			Columns: num_fields,
		},
	}, nil
}

// Count returns the number of rows in the DataFrame.
func (d *DataFrame) Count() int {
	return d.Data.getNumberOfRows()
}

// Distinct returns a list of unique values in the specified field.
func (d *DataFrame) Distinct(fieldname string) []any {
	x := d.Schema.GetField(fieldname)
	if x < 0 {
		return nil
	}
	indexer := make(map[any]bool)
	field := d.Data.getColumn(x)
	if field == nil {
		return nil
	}
	if len(field) == 0 {
		return nil
	}
	// Iterate over the field and add unique values to the indexer
	for _, v := range field {
		indexer[v] = true
	}

	keys := make([]any, 0)
	for k := range indexer {
		keys = append(keys, k)
	}
	return keys
}
