package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

type fileChunk struct {
	size, offset int64
}

// splitFile splits a file into multiple chunks based on the given count
//
// It takes the file path and count as input parameters and returns a slice of fileChunk and an error.
//
// The function opens the file specified by the filePath using os.OpenFile and reads its size with os.Stat.
// It then calculates the chunk size based on the file size and count.
// The function creates a byte buffer with maxLineLength to store chunks of data.
//
// It iterates count number of times to create chunks. If it's the last iteration, it creates a chunk with the remaining data.
// For previous iterations, it seeks to the appropriate offset in the file using file.Seek and reads the data into the buffer.
// It finds the last newline character in the data and creates a chunk based on the remaining data.
// The offset is updated for the next iteration.
//
// Finally, it returns the chunks and any encountered error.
func splitFile(filePath string, count int) ([]fileChunk, error) {
	const maxLineLength = 100

	file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err

	}

	size := stat.Size()
	chunkSize := size / int64(count)

	buf := make([]byte, maxLineLength)
	chunks := make([]fileChunk, 0, count)
	offset := int64(0)

	for i := 0; i < count; i++ {
		if i == (count - 1) {
			if offset < size {
				chunks = append(chunks, fileChunk{size - offset, offset})
			}
			break
		}

		seekOffset := max(offset+chunkSize-maxLineLength, 0)
		_, err := file.Seek(seekOffset, io.SeekStart)
		if err != nil {
			return nil, err
		}

		n, _ := io.ReadFull(file, buf)
		chunk := buf[:n]
		nl := bytes.LastIndexByte(chunk, '\n')
		if nl < 1 {
			return nil, fmt.Errorf("newline not found at offset %d", offset+chunkSize-maxLineLength)
		}

		remaining := len(chunk) - nl - 1
		nextOffset := seekOffset + int64(len(chunk)) - int64(remaining)
		chunks = append(chunks, fileChunk{nextOffset - offset, offset})
		offset = nextOffset
	}
	return chunks, nil
}

func processChuckS1(filePath string, fileOffset, fileSize int64, resultChan chan map[string]*WeatherStationStats) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = file.Seek(fileOffset, io.SeekStart)
	if err != nil {
		panic(err)
	}
	flr := io.LimitedReader{R: file, N: fileSize}

	weatherData := NewWeatherData()

	scanner := bufio.NewScanner(&flr)
	for scanner.Scan() {
		line := scanner.Text()
		row := strings.Split(line, ";")

		if len(row) > 0 {
			station := row[0]
			tempStr := row[1]

			temp, err := strconv.ParseFloat(tempStr, 64)
			if err != nil {
				panic(err)
			}

			if stat := weatherData.data[station]; stat != nil {
				if stat.min > temp {
					stat.min = temp
				}

				if stat.max < temp {
					stat.max = temp
				}

				stat.count++
				stat.sum += temp
			} else {
				weatherData.data[station] = &WeatherStationStats{
					min:   temp,
					max:   temp,
					count: 1,
					sum:   temp,
				}
			}
		}
	}
	resultChan <- weatherData.data
}

func solution3(filePath string, output io.Writer) error {
	maxGoroutines := runtime.NumCPU()
	chunks, err := splitFile(filePath, maxGoroutines)
	if err != nil {
		return err
	}

	resultsChan := make(chan map[string]*WeatherStationStats)
	for _, chunk := range chunks {
		go processChuckS1(filePath, chunk.offset, chunk.size, resultsChan)
	}

	weatherData := make(map[string]*WeatherStationStats)
	for i := 0; i < len(chunks); i++ {
		for station, stat := range <-resultsChan {
			ts, ok := weatherData[station]
			if !ok {
				weatherData[station] = &WeatherStationStats{
					min:   stat.min,
					max:   stat.max,
					sum:   stat.sum,
					count: stat.count,
				}
				continue
			}

			ts.min = min(ts.min, stat.min)
			ts.max = min(ts.max, stat.max)
			ts.sum += stat.sum
			ts.count += stat.count
			weatherData[station] = ts
		}
	}

	weatherStations := make([]string, 0, len(weatherData))
	for station := range weatherData {
		weatherStations = append(weatherStations, station)
	}
	sort.Strings(weatherStations)

	fmt.Fprint(output, "{")
	for i, station := range weatherStations {
		if i > 0 {
			fmt.Fprint(output, ", ")
		}

		stat := weatherData[station]
		mean := stat.sum / float64(stat.count)
		fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f", station, stat.min, mean, stat.max)
	}
	fmt.Fprintln(output, "}")
	return nil
}
