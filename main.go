package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"time"
)

type solutionFunc func(string, io.Writer) error

var solutions = []solutionFunc{solution1}

func benchmark(filePath string) error {
	const MaxTries = 5

	var output1 bytes.Buffer // use buffer as output not to clutter the stdout
	err := solution1(filePath, &output1)
	if err != nil {
		return err
	}

	var s1Best time.Duration

	for i, solution := range solutions {
		fmt.Printf("solution%d: ", i+1)
		bestTime := time.Duration(math.MaxInt64)

		for trial := 0; trial < MaxTries; trial++ {
			var output2 bytes.Buffer
			start := time.Now()
			err := solution(filePath, &output2)
			if err != nil {
				return err
			}
			elapsed := time.Since(start)
			fmt.Fprintf(os.Stdout, " %v", elapsed)
			bestTime = min(bestTime, elapsed)
			if i == 0 {
				s1Best = bestTime
			}
		}

		fmt.Fprintf(os.Stdout, " - best: %v (%.2fx faster than solution1)\n",
			bestTime, float64(s1Best)/float64(bestTime))
	}
	return nil
}

func main() {
	var filePath string
	flag.StringVar(&filePath, "file", "", "Path to the weather station data file")
	flag.Parse()

	if filePath == "" {
		fmt.Fprintln(os.Stderr, "Error: Required flag '-file' is missing")
		flag.Usage()
		os.Exit(1)
	}

	err := benchmark(filePath)
	if err != nil {
		log.Fatalln(err)
	}
}
