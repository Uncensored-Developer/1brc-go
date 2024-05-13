package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime/pprof"
	"time"
)

type solutionFunc func(string, io.Writer) error

var solutions = []solutionFunc{solution1, solution2}

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
	var cpuProfilePath string
	var solution int

	var err error

	flag.StringVar(&filePath, "file", "", "Path to the weather station data file")
	flag.StringVar(&cpuProfilePath, "cpu_profile", "", "Path to save CPU profile to")
	flag.IntVar(&solution, "solution", 0, "Solution to run")
	flag.Parse()

	if filePath == "" {
		fmt.Fprintln(os.Stderr, "Error: Required flag '-file' is missing")
		flag.Usage()
		os.Exit(1)
	}

	if cpuProfilePath != "" {
		profileFile, err := os.Create(cpuProfilePath)
		if err != nil {
			log.Fatalln(err)
		}
		pprof.StartCPUProfile(profileFile)
		defer pprof.StopCPUProfile()
	}

	switch {
	case solution == 0:
		err = benchmark(filePath)
		if err != nil {
			log.Fatalln(err)
		}
	case solution < 1 || solution > len(solutions):
		fmt.Fprintf(
			os.Stderr,
			"Error: Invalid solution, should be between 1 and %d\n",
			len(solutions))
		os.Exit(1)
	default:
		start := time.Now()
		var output bytes.Buffer
		solFunc := solutions[solution-1]
		err = solFunc(filePath, &output)
		if err != nil {
			log.Fatalln(err)
		}
		elapsed := time.Since(start)

		fmt.Fprintf(
			os.Stdout,
			"Solution%d ran in %s\n",
			solution, elapsed,
		)
	}
}
