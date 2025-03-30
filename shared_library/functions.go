package sharedlibrary

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"text/tabwriter"

	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/types"
)

// transpose transposes a slice of Data, converting rows to columns and vice versa.
func transpose(slice []*Data) [][]any {
	len_array := len(*slice[0])
	len_matrix := len(slice)
	result := make([][]any, len_matrix)
	for i := range result {
		result[i] = make([]any, len_array)
	}

	for i := 0; i < len_array; i++ {
		for j := 0; j < len_matrix; j++ {
			r := *slice[j]
			result[j][i] = r[i]
		}
	}
	return result
}

// transpose transposes a 2D slice of strings.
//
// Parameters:
//   - slice: A 2D slice of strings to transpose.
//
// Returns:
//   - A 2D slice of strings that is the transposed version of the input slice.
//
// Note:
//   - The function assumes that the input slice is rectangular.

func transposeStrArrays(slice [][]string) [][]string {
	xl := len(slice[0])
	yl := len(slice)
	result := make([][]string, xl)
	for i := range result {
		result[i] = make([]string, yl)
	}
	for i := 0; i < xl; i++ {
		for j := 0; j < yl; j++ {
			result[i][j] = slice[j][i]
		}
	}
	return result
}

// ToAnyList converts a generic slice to a slice of any type.
func ToAnyList[T any](input []T) []any {
	list := make([]any, len(input))
	for i, v := range input {
		list[i] = v
	}
	return list
}

func PrintDataframe(b DataFrame, f *os.File) {
	l := 0
	if b.GetNumberOfRows() > 1000 {
		l = 1000
	} else {
		l = b.GetNumberOfRows()
	}

	if l == 0 {
		log.Fatal("no data")
	}
	// Create a tabwriter to format the output
	w := tabwriter.NewWriter(f, 0, 0, 0, '.', tabwriter.Debug)
	fmt.Fprintln(w, "------- DUMP START --------")

	fmt.Fprint(w, "\t")
	for _, v := range b.GetFieldNames() {
		fmt.Fprintf(w, "%s\t", v)
	}
	fmt.Fprintln(w)
	for i := 0; i < l; i++ {
		fmt.Fprint(w, "\t")
		for j := range b.GetNumberOfColumns() {
			v, err := b.GetPositionValue(j, i)
			if err != nil {
				log.Fatal("nil value")
			}
			fmt.Fprintf(w, " %v\t", v)
		}
		fmt.Fprintln(w)
	}
	fmt.Fprintln(w, "------- DUMP END --------")
	w.Flush()
}

// createDataFrame reads all records from a CSV reader and constructs a DataFrame.
// It initializes the schema of the DataFrame based on the header row of the CSV,
// where each column is treated as a string field. The function also transposes
// the remaining rows of the CSV to align with the schema and converts them into
// a format suitable for the DataFrame.
//
// Parameters:
//   - r: A pointer to a csv.Reader instance that provides the CSV data.
//
// Returns:
//   - A lib.DataFrame instance containing the schema and data extracted from the CSV.
//
// Note:
//   - The function logs a fatal error and exits the program if reading the CSV fails.

func CreateDataFrameFromCSV(r *csv.Reader) DataFrame {
	recs, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	var Fields []Field
	for i, v := range recs[0] {
		Fields = append(Fields, Field{
			FieldName:     v,
			FieldPosition: i,
			FieldType:     "string",
		})
	}
	new_recs := transposeStrArrays(recs[1:])
	data := make([]*Data, 0)
	for _, v := range new_recs {
		lst := ToAnyList(v)
		data = append(data, &lst)
	}

	d := NewDataFrameWithArgs(Fields, data)
	return *d
}

func CreateDataFrameFromParquet(r *reader.ParquetReader) DataFrame {

	rootPath := r.SchemaHandler.SchemaElements[0].Name

	var Fields []Field

	for i, v := range r.SchemaHandler.SchemaElements[1:] {
		Fields = append(Fields, Field{
			FieldName:     v.Name,
			FieldPosition: i,
			FieldType:     v.GetType().String(),
		})
	}

	data := make([]*Data, 0)

	len_rows := r.GetNumRows()
	for _, v := range Fields {
		value, _, _, err := r.ReadColumnByPath(common.ReformPathStr(rootPath+"."+v.FieldName), len_rows)
		if err != nil {
			log.Fatal(err)
		}

		if v.FieldType == "INT96" {
			new_time_array := make([]any, len_rows)
			for i, v2 := range value {
				new_time_array[i] = types.INT96ToTime(v2.(string))
			}
			v.FieldType = "time.Time"
			value = new_time_array
		}
		fmt.Println(v.FieldName, v.FieldType)
		data = append(data, &value)

	}
	d := NewDataFrameWithArgs(Fields, data)
	return *d
}
