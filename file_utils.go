package main

import "net/http"
import "io/ioutil"

import "bufio"
import "strings"

import "strconv"

// Point holds x, y coordinates in degrees
type Point struct {
	x float64
	y float64
}

func downloadFile(path string) ([]byte, error) {
	r, err := http.Get(path)

	if err != nil {
		return nil, err
	}

	defer r.Body.Close()

	data, err := ioutil.ReadAll(r.Body)

	return data, err
}

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

		x, err := strconv.ParseFloat(arr[0], 64)
		if err != nil {
			return nil, err
		}
		y, err := strconv.ParseFloat(arr[1], 64)
		if err != nil {
			return nil, err
		}

		points = append(points, Point{x: x, y: y})
	}

	return points, nil
}
