package main

import (
	"image"
	"image/jpeg"
	"net/http"
)

func getBandRating(url string, band string) (int, error) {
	// You can register another format here
	image.RegisterFormat("jp2", "jp2", jpeg.Decode, jpeg.DecodeConfig)

	// pixels, err := getPixels(url)

	// if err != nil {
	// 	return 0, err
	// }

	return 0, nil
}

// Get the bi-dimensional pixel array
func getPixels(url string) ([][]Pixel, error) {

	resp, err := http.Get(url)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	img, _, err := image.Decode(resp.Body)

	if err != nil {
		return nil, err
	}

	bounds := img.Bounds()
	width, height := bounds.Max.X, bounds.Max.Y

	var pixels [][]Pixel

	for y := 0; y < height; y++ {
		var row []Pixel
		for x := 0; x < width; x++ {
			row = append(row, rgbaToPixel(img.At(x, y).RGBA()))
		}
		pixels = append(pixels, row)
	}
	return pixels, nil
}

// img.At(x, y).RGBA() returns four uint32 values; we want a Pixel
func rgbaToPixel(r uint32, g uint32, b uint32, a uint32) Pixel {
	return Pixel{int(r / 257), int(g / 257), int(b / 257), int(a / 257)}
}

// Pixel struct example
type Pixel struct {
	R int
	G int
	B int
	A int
}
