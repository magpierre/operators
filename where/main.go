package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"os"

	lib "github.com/magpierre/operators/shared_library"
)

var (
	file   = flag.String("file", "", "file to read")
	col    = flag.String("col", "", "column to filter")
	cond   = flag.String("cond", "", "value to keep")
	output = flag.Bool("debug", false, "Dump output to stderr")
)

func main() {
	flag.Parse()
	gob.Register(&lib.DataFrame{})
	gob.Register(&lib.InternalDataStructure{})
	var f *bufio.Reader
	if *file == "" {
		f = bufio.NewReader(os.Stdin)
	} else {
		file, err := os.Open(*file)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		f = bufio.NewReader(file)
	}

	var b lib.DataFrame
	fd := gob.NewDecoder(f)

	err := fd.Decode(&b)
	if err != nil {
		log.Fatal(err)
	}

	df, err := b.Where(*cond)
	if err != nil {
		log.Fatal(err)
	}

	if *output {
		l := df.GetNumberOfRows()
		for i := 0; i < l; i++ {
			fmt.Fprint(os.Stderr, "| ")
			for j := range df.GetNumberOfColumns() {
				v, err := df.GetPositionValue(i, j)
				if err != nil {
					log.Fatal("nil value")
				}
				fmt.Fprintf(os.Stderr, " %v |", v)
				//fmt.Fprintf(os.Stderr, " %v |", (*v)[i])
			}
			fmt.Fprintln(os.Stderr)
		}
	}

	encoder := gob.NewEncoder(os.Stdout)
	encoder.Encode(df)
}
