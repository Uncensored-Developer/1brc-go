package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
)

type weatherStationStats struct {
	min, max, sum int32
	count         int
}

type weatherData struct {
	data map[string]*weatherStationStats
}

func NewweatherData() weatherData {
	return weatherData{data: make(map[string]*weatherStationStats)}
}

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

	weatherData := NewweatherData()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()

		var temp int32
		var delimeter int
		end := len(line)
		tenthsDigit := int32(line[end-1] - '0')
		onesDigit := int32(line[end-3] - '0')

		if line[end-4] == ';' { // X.X temperature
			temp = onesDigit*10 + tenthsDigit
			delimeter = end - 4
		} else if line[end-4] == '-' { // -X.X temperature
			temp = -(onesDigit*10 + tenthsDigit)
			delimeter = end - 5
		} else {
			tens := int32(line[end-4] - '0')
			if line[end-5] == ';' { // XX.X temperature
				temp = tens*100 + onesDigit*10 + tenthsDigit
				delimeter = end - 5
			} else { // -XX.X temperature
				temp = -(tens*100 + onesDigit*10 + tenthsDigit)
				delimeter = end - 6
			}
		}
		station := string(line[:delimeter])

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
			weatherData.data[station] = &weatherStationStats{
				min:   temp,
				max:   temp,
				count: 1,
				sum:   temp,
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
		mean := float64(stat.sum) / float64(stat.count) / 10
		fmt.Fprintf(
			output,
			"%s=%.1f/%.1f/%.1f",
			station, float64(stat.min)/10, mean, float64(stat.max)/10)
	}
	fmt.Fprintln(output, "}")
	return nil
}
