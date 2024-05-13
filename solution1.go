package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

type WeatherStationStats struct {
	min, max, sum float64
	count         int
}

type WeatherData struct {
	data map[string]*WeatherStationStats
}

func NewWeatherData() WeatherData {
	return WeatherData{data: make(map[string]*WeatherStationStats)}
}

func solution1(filePath string, output io.Writer) error {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	weatherData := NewWeatherData()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		row := strings.Split(line, ";")

		if len(row) > 0 {
			station := row[0]
			tempStr := row[1]

			temp, err := strconv.ParseFloat(tempStr, 64)
			if err != nil {
				fmt.Fprintf(output, "Error converting tempStr to float64: %s\n", err.Error())
				continue
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
