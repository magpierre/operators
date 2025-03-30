package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"log"
	"os"
	"strings"

	lib "github.com/magpierre/operators/shared_library"
)

var (
	cols = flag.String("cols", "", "Comma separated list of columns")
)

func main() {

	flag.Parse()
	columns := strings.Split(*cols, ",")

	gob.Register(&lib.DataFrame{})
	gob.Register(&lib.InternalDataStructure{})

	f := bufio.NewReader(os.Stdin)
	var b lib.DataFrame
	fd := gob.NewDecoder(f)

	err := fd.Decode(&b)
	if err != nil {
		log.Fatal(err)
	}

	df, err := b.Project(columns...)
	if err != nil {
		log.Fatal("While performing project:", err)
	}

	encoder := gob.NewEncoder(os.Stdout)

	encoder.Encode(df)
}
