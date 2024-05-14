package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
)

type s5WeatherStationStats struct {
	min, max, count int32
	sum             int64
}

func processChuckS5(filePath string, fileOffset, fileSize int64, resultChan chan map[string]*s5WeatherStationStats) {
	type hashTable struct {
		key   []byte
		value *s5WeatherStationStats
	}

	const bucketsCount = 1 << 17 // number of hash buckets (power of 2)
	items := make([]hashTable, bucketsCount)
	size := 0

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

	//weatherData := NewWeatherData()

	buf := make([]byte, 1024*1024) // allocate 1MB buffer to store file chunks
	start := 0

	for {
		nb, err := flr.Read(buf[start:])
		if err != nil && err != io.EOF {
			panic(err)
		}
		if start+nb == 0 {
			break
		}
		chunk := buf[:start+nb]

		nl := bytes.LastIndexByte(chunk, '\n')
		if nl < 0 {
			break
		}

		left := chunk[nl+1:]
		chunk = chunk[:nl+1]

		for {
			// FNV-1 constants from hash/fnv
			const (
				offset64 = 14695981039346656037
				prime64  = 1099511628211
			)

			// Hash the station name and look for ';'
			var station, tempBytes []byte
			hash := uint64(offset64)
			i := 0
			for ; i < len(chunk); i++ {
				c := chunk[i]
				if c == ';' {
					station = chunk[:i]
					tempBytes = chunk[i+1:]
					break
				}
				hash ^= uint64(c)
				hash *= prime64
			}
			if i == len(chunk) {
				break
			}

			negative := false
			idx := 0

			if tempBytes[idx] == '-' {
				negative = true
				idx++
			}

			// Parse the first digit
			tempFlt := int32(tempBytes[idx] - '0')
			idx++

			// Parse the second digit (optional).
			if tempBytes[idx] != '.' {
				tempFlt = tempFlt*10 + int32(tempBytes[idx]-'0')
				idx++
			}
			idx++

			tempFlt = tempFlt*10 + int32(tempBytes[idx]-'0')
			idx += 2
			if negative {
				tempFlt = -tempFlt
			}
			chunk = tempBytes[idx:]

			hashIdx := int(hash & uint64(bucketsCount-1))
			for {
				if items[hashIdx].key == nil {
					// Found an empty slot, add new item
					key := make([]byte, len(station))
					copy(key, station)

					items[hashIdx] = hashTable{
						key: key,
						value: &s5WeatherStationStats{
							min:   tempFlt,
							max:   tempFlt,
							sum:   int64(tempFlt),
							count: 1,
						},
					}
					size++
					break
				}

				if bytes.Equal(items[hashIdx].key, station) {
					// Found matching slot, update stats
					stat := items[hashIdx].value
					stat.min = min(stat.min, tempFlt)
					stat.max = max(stat.max, tempFlt)
					stat.sum += int64(tempFlt)
					stat.count++
					break
				}

				// Another key already in slot, try next slot (linear probe)
				hashIdx++
				if hashIdx >= bucketsCount {
					hashIdx = 0
				}
			}
		}
		start = copy(buf, left)
	}

	stats := make(map[string]*s5WeatherStationStats, size)
	for _, item := range items {
		if item.key == nil {
			continue
		}
		stats[string(item.key)] = item.value
	}
	resultChan <- stats
}

func solution5(filePath string, output io.Writer) error {
	maxGoroutines := runtime.NumCPU()
	chunks, err := splitFile(filePath, maxGoroutines)
	if err != nil {
		return err
	}

	resultsChan := make(chan map[string]*s5WeatherStationStats)
	for _, chunk := range chunks {
		go processChuckS5(filePath, chunk.offset, chunk.size, resultsChan)
	}

	weatherData := make(map[string]*s5WeatherStationStats)
	for i := 0; i < len(chunks); i++ {
		for station, stat := range <-resultsChan {
			ts := weatherData[station]
			if ts == nil {
				weatherData[station] = stat
				continue
			}

			ts.min = min(ts.min, stat.min)
			ts.max = max(ts.max, stat.max)
			ts.sum += stat.sum
			ts.count += stat.count
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
		mean := float64(stat.sum) / float64(stat.count) / 10
		fmt.Fprintf(
			output,
			"%s=%.1f/%.1f/%.1f",
			station,
			float64(stat.min)/10, mean, float64(stat.max)/10)
	}
	fmt.Fprintln(output, "}")
	return nil
}
