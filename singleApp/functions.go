package main

import (
	"encoding/csv"
	"flag"
	"io"
	"log"
	"os"
)

// isFlagPassed checks if a flag is passed to the program.
//
// Parameters:
//   - name: A string containing the name of the flag to check.
//
// Returns:
//   - A boolean value indicating whether the flag is passed to the program.

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
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

// readCSV reads a CSV file and returns a pointer to a csv.Reader instance.
// If the file flag is not passed, the function reads from standard input.
//
// Parameters:
//   - f: A string containing the file path to read from.
//
// Returns:
//   - A pointer to a csv.Reader instance that provides the CSV data.
//
// Note:
//   - The function logs a fatal error and exits the program if the file cannot be opened.
//   - The function logs a fatal error and exits the program if the file flag is passed but not found.

func readCSV(f string) *csv.Reader {
	var fp io.Reader
	var r *csv.Reader
	var err error
	if !isFlagPassed("file") {
		r = csv.NewReader(os.Stdin)
	} else {
		fp, err = os.Open(f)
		if err != nil {
			log.Fatal(err)
		}
		r = csv.NewReader(fp)
	}
	return r
}
