package main

import (
	"bufio"
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

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()
		row := bytes.Split(line, []byte(";"))

		if len(row) > 0 {
			station := string(row[0])
			tempBytes := row[1]

			temp := parseTemperature(tempBytes)

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

	if err := scanner.Err(); err != nil {
		return err
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
		fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f", station, stat.min, mean, stat.max)
	}
	fmt.Fprintln(output, "}")
	return nil
}
