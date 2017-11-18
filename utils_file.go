package main

import "net/http"
import "io/ioutil"

import "bufio"
import "strings"

import "strconv"

// Point holds latitude, longitude coordinates in degrees
type Point struct {
	lat float64
	lng float64
}

// Download a file from a specified path
func downloadFile(path string) ([]byte, error) {
	r, err := http.Get(path)

	if err != nil {
		return nil, err
	}

	defer r.Body.Close()

	data, err := ioutil.ReadAll(r.Body)

	return data, err
}

// Parse .poly file line by line from a byte array
func parsePolyData(data []byte) ([]Point, error) {
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Split(bufio.ScanLines)

	var line string

	points := make([]Point, 0)

	for scanner.Scan() {
		line = strings.TrimSpace(scanner.Text())

		if line == "none" || line == "END" || line == "1" {
			continue
		}

		arr := strings.Split(line, "   ")

		lat, err := strconv.ParseFloat(arr[1], 32)
		if err != nil {
			return nil, err
		}
		lng, err := strconv.ParseFloat(arr[0], 32)
		if err != nil {
			return nil, err
		}

		points = append(points, Point{lat: lat, lng: lng})
	}

	return points, nil
}
