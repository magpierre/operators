package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"text/tabwriter"

	lib "github.com/magpierre/operators/shared_library"
)

func main() {
	r := io.TeeReader(os.Stdin, os.Stdout)
	f := bufio.NewReader(r)

	gob.Register(&lib.DataFrame{})
	gob.Register(&lib.InternalDataStructure{})

	var b lib.DataFrame
	fd := gob.NewDecoder(f)

	err := fd.Decode(&b)
	if err != nil {
		log.Fatal(err)
	}

	l := b.GetNumberOfRows()
	if l == 0 {
		log.Fatal("no data")
		return
	}
	w := tabwriter.NewWriter(os.Stderr, 0, 0, 0, '.', tabwriter.Debug)
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
				log.Fatal("nil value", j, i)
			}
			fmt.Fprintf(w, " %v\t", v)
		}
		fmt.Fprintln(w)
	}
	fmt.Fprintln(w, "------- DUMP END --------")
	w.Flush()

}
