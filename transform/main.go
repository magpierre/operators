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
	file      = flag.String("file", "", "file to read")
	statement = flag.String("statement", "", "value to keep")
	output    = flag.Bool("debug", false, "Dump output to stderr")
)

func main() {
	// This is a placeholder for the main function
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

	b.IndexRows()
	err = b.Transform(*statement)
	if err != nil {
		log.Fatal(err)
	}

	if *output {
		l := b.GetNumberOfColumns()
		for i := 0; i < l; i++ {
			fmt.Fprint(os.Stderr, "| ")
			for j := range b.GetNumberOfRows() {
				v, err := b.GetPositionValue(i, j)
				if err != nil {
					log.Fatal("nil value")
				}
				fmt.Fprintf(os.Stderr, " %v |", v)
			}
			fmt.Fprintln(os.Stderr)
		}
	}

	encoder := gob.NewEncoder(os.Stdout)
	encoder.Encode(b)
}
