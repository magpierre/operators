package main

import (
	"encoding/csv"
	"encoding/gob"
	"flag"
	"io"
	"log"
	"os"

	lib "github.com/magpierre/operators/shared_library"
)

type ImportOpts struct {
	file              string
	schema            string
	schemaFile        string
	checkpoint        int
	offsetWithSources bool
	filterCmd         string
	firstN            int
}

var (
	f                 = flag.String("file", "", "the file to import")
	schema            = flag.String("schema", "", "The schema of the input file")
	schemaFile        = flag.String("schemaFile", "file-path", "The path to a file that contains the schema")
	checkpoint        = flag.Int("checkpoint", 0, "Checkpoints")
	offsetWithSources = flag.Bool("dontUseOffsetsWithSources", true, "Offsets with sources")
	filterCmd         = flag.String("filter", "", "Filter Command")
	firstN            = flag.Int("first", 0, "Read first N lines")
)

func transpose(slice [][]string) [][]string {
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

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func main() {
	flag.Parse()
	i := ImportOpts{
		file:              *f,
		schema:            *schema,
		schemaFile:        *schemaFile,
		checkpoint:        *checkpoint,
		offsetWithSources: *offsetWithSources,
		filterCmd:         *filterCmd,
		firstN:            *firstN,
	}

	var fp io.Reader
	var r *csv.Reader
	var err error
	if !isFlagPassed("file") {
		r = csv.NewReader(os.Stdin)
	} else {
		fp, err = os.Open(i.file)
		if err != nil {
			log.Fatal(err)
			return
		}
		r = csv.NewReader(fp)
	}

	recs, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
		return
	}

	var Fields []lib.Field
	for i, v := range recs[0] {
		Fields = append(Fields, lib.Field{
			FieldName:     v,
			FieldPosition: i,
			FieldType:     "string",
		})
	}

	new_recs := transpose(recs[1:])
	data := make([]*lib.Data, 0)
	for _, v := range new_recs {
		lst := lib.ToAnyList(v)
		data = append(data, &lst)
	}

	// Create a DataFrame
	d := lib.NewDataFrameWithArgs(Fields, data)
	d.IndexRows()

	gob.Register(&lib.DataFrame{})
	gob.Register(&lib.InternalDataStructure{})
	encoder := gob.NewEncoder(os.Stdout)
	encoder.Encode(*d)

}
