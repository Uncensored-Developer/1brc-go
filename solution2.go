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
			station, tempBytes, hasDelimeter := bytes.Cut(chunk, []byte(";"))
			if !hasDelimeter {
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

			if stat := weatherData.data[string(station)]; stat != nil {
				if stat.min > tempFlt {
					stat.min = tempFlt
				}

				if stat.max < tempFlt {
					stat.max = tempFlt
				}

				stat.count++
				stat.sum += tempFlt
			} else {
				weatherData.data[string(station)] = &WeatherStationStats{
					min:   tempFlt,
					max:   tempFlt,
					count: 1,
					sum:   tempFlt,
				}
			}
		}
		start = copy(buf, left)
	}

	weatherStations := make([]string, 0, len(weatherData.data))
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
