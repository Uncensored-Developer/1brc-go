package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
)

func parseTemperature(temp []byte) float64 {
	negative := false
	idx := 0

	if temp[idx] == '-' {
		negative = true
		idx++
	}

	// Parse the first digit
	tempFlt := float64(temp[idx] - '0')
	idx++

	// Parse the second digit (optional).
	if temp[idx] != '.' {
		tempFlt = tempFlt*10 + float64(temp[idx]-'0')
		idx++
	}
	idx++

	tempFlt += float64(temp[idx]-'0') / 10 // convert to decimal
	if negative {
		tempFlt = -tempFlt
	}
	return tempFlt
}

func solution2(filePath string, output io.Writer) error {

	type hashTable struct {
		key   []byte
		value *WeatherStationStats
	}

	const bucketsCount = 1 << 17 // number of hash buckets (power of 2)
	items := make([]hashTable, bucketsCount)
	size := 0

	file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	weatherData := NewWeatherData()

	buf := make([]byte, 1024*1024) // allocate 1MB buffer to store file chunks
	start := 0

	for {
		nb, err := file.Read(buf[start:])
		if err != nil && err != io.EOF {
			return err
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

			//var temp float64
			negative := false
			idx := 0

			if tempBytes[idx] == '-' {
				negative = true
				idx++
			}

			// Parse the first digit
			tempFlt := float64(tempBytes[idx] - '0')
			idx++

			// Parse the second digit (optional).
			if tempBytes[idx] != '.' {
				tempFlt = tempFlt*10 + float64(tempBytes[idx]-'0')
				idx++
			}
			idx++

			tempFlt += float64(tempBytes[idx]-'0') / 10 // convert to decimal
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
						value: &WeatherStationStats{
							min:   tempFlt,
							max:   tempFlt,
							sum:   tempFlt,
							count: 1,
						},
					}
					size++

					//if size > bucketsCount/2 {
					//	panic("hash table overflow")
					//}
					break
				}

				if bytes.Equal(items[hashIdx].key, station) {
					// Found matching slot, update stats
					stat := items[hashIdx].value
					stat.min = min(stat.min, tempFlt)
					stat.max = max(stat.max, tempFlt)
					stat.sum += tempFlt
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

	weatherStations := make([]string, 0, size)
	for station := range weatherData.data {
		weatherStations = append(weatherStations, station)
	}
	sort.Strings(weatherStations)

	fmt.Fprint(output, "{")
	for i, station := range weatherStations {
		if i > 0 {
			fmt.Fprint(output, ", ")
		}

		stat := weatherData.data[station]
		mean := stat.sum / float64(stat.count)
		fmt.Fprintf(
			output,
			"%s=%.1f/%.1f/%.1f",
			station, stat.min, mean, stat.max)
	}
	fmt.Fprintln(output, "}")
	return nil
}
