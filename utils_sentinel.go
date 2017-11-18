package main

import (
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/golang/geo/s2"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

// Get images from all granules bounded by a polygon
func getPolygonImages(degreePoints []Point) (int, error) {
	points := make([]s2.Point, 0)

	// Order matters here!
	for i := len(degreePoints) - 1; i >= 0; i-- {
		points = append(points, s2.PointFromLatLng(s2.LatLngFromDegrees(degreePoints[i].lat, degreePoints[i].lng)))
	}

	l1 := s2.LoopFromPoints(points)
	loops := []*s2.Loop{l1}
	poly := s2.PolygonFromLoops(loops)

	rc := &s2.RegionCoverer{MaxLevel: 30, MaxCells: 20}
	cover := rc.Covering(poly)

	count := 0
	var rect s2.Rect
	var err error
	var urls []string

	chAbort := make(chan error)
	chCount := make(chan int)

	fmt.Printf("Channel amount: %v \n", len(cover))

	for i := 0; i < len(cover); i++ {
		rect = s2.CellFromCellID(cover[i]).RectBound()

		// Execute each request concurrently
		go func(rect s2.Rect, chCount chan int, chAbort chan error) {
			urls, err = getImageURLs(strconv.FormatFloat(rect.RectBound().Lat.Lo*180.0/math.Pi, 'f', 6, 64),
				strconv.FormatFloat(rect.RectBound().Lng.Lo*180.0/math.Pi, 'f', 6, 64),
				strconv.FormatFloat(rect.RectBound().Lat.Hi*180.0/math.Pi, 'f', 6, 64),
				strconv.FormatFloat(rect.RectBound().Lng.Hi*180.0/math.Pi, 'f', 6, 64))

			fmt.Printf("Request for LatLO: %v, LatHI: %v, LngLO: %v, LngHI: %v completed with count: %v \n",
				rect.RectBound().Lat.Lo*180.0/math.Pi,
				rect.RectBound().Lat.Hi*180.0/math.Pi,
				rect.RectBound().Lng.Lo*180.0/math.Pi,
				rect.RectBound().Lng.Hi*180.0/math.Pi,
				len(urls))

			if err != nil {
				chAbort <- err
			} else {
				chCount <- len(urls)
			}
		}(rect, chCount, chAbort)
	}

	// Should get back exactly len(cover) messages
	for i := 0; i < len(cover); i++ {
		select {
		case err := <-chAbort:
			return 0, err
		case c := <-chCount:
			count += c
			fmt.Printf("Total count increased to: %v \n", count)
		}
	}

	fmt.Println("**********Finished!!!!")

	return count, nil
}

// Get images in all granules intersecting specific geo bound
// The "bound" can be specified as a single latitude/longitude coordinate or a tuple of latitude/longitude coordinates
func getImageURLs(lat1 string, lng1 string, lat2 string, lng2 string) ([]string, error) {

	var dbit *bigquery.RowIterator
	var err error

	if lat2 == "" && lng2 == "" {
		dbit, err = getBaseURLs(lng1, lat1)
	} else {
		dbit, err = getBaseURLs2(lng1, lat1, lng2, lat2)
	}

	if err != nil {
		return nil, err
	}

	links := make([]string, 0)

	// This seems ridiculous but the bigquery.RowIterator does not seem to have a field with the number of rows
	rowCount := 0

	chReq := make(chan GranuleRequest)
	chResp := make(chan GranuleResult, 100)
	chAbort := make(chan error)

	// This can deadlick if the second argument is not big enough since the worker buffer gets filled-up,
	// however nobody is taking out the responses
	balancer := NewBalancer(100, 50)

	// Start-up the load balancer
	go balancer.Balance(chReq, chAbort)

	for {
		var row []bigquery.Value

		err := dbit.Next(&row)

		if err == iterator.Done {
			break
		}

		rowCount++

		if err != nil {
			return nil, err
		}

		url := strings.TrimPrefix(row[0].(string), "gs://gcp-public-data-sentinel-2/") + "/GRANULE/" + row[1].(string) + "/IMG_DATA/"

		chReq <- GranuleRequest{ch: chResp, url: url, fn: getImages}
	}

	// Wait for responses from all go-routines
	for i := 0; i < rowCount; i++ {
		select {
		case resp := <-chResp:
			if resp.err != nil {
				chAbort <- resp.err
				return nil, resp.err
			}
			links = append(links, resp.urls...)
		}
	}

	fmt.Println("Finishing!!!")

	return links, nil
}

// Get images for a specific path from Google Cloud Storage
func getImages(_path string) GranuleResult {

	res := GranuleResult{}

	ctx := context.Background()
	urls := make([]string, 0)

	query := &storage.Query{Prefix: _path}

	it := StorageBucket.Objects(ctx, query)

	for {
		objAttrs, err := it.Next()

		if err == iterator.Done {
			break
		}

		if err != nil {
			res.err = err
			return res
		}

		// Get only .jp2 files
		if path.Ext(objAttrs.Name) == ".jp2" {
			urls = append(urls, objAttrs.MediaLink)
		}
	}

	res.urls = urls
	return res
}

// Get urls for all granules intersecting single (latitude, longitude) coordinate
func getBaseURLs(lng string, lat string) (*bigquery.RowIterator, error) {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, "avon-178408")

	if err != nil {
		return nil, err
	}

	sql := fmt.Sprintf(`SELECT base_url, granule_id 
		FROM`+" `bigquery-public-data.cloud_storage_geo_index.sentinel_2_index` "+
		`WHERE south_lat <= %s AND north_lat >= %s AND west_lon <= %s AND east_lon >= %s;`,
		lat, lat, lng, lng)

	query := client.Query(sql)
	query.QueryConfig.UseStandardSQL = true

	return query.Read(ctx)
}

// Get urls for all granules intersecting two (latitude, longitude) coordinates
func getBaseURLs2(lng1 string, lat1 string, lng2 string, lat2 string) (*bigquery.RowIterator, error) {

	var err error

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, "avon-178408")

	if err != nil {
		return nil, err
	}

	// The granule intersects any of the (top, left), (bottom, left), (top, right), (bottom, right) corners of the input-bounds
	// The input-bounds is fully within the granule
	// The granule bounds is fully contained in input-bounds
	sql := fmt.Sprintf(`SELECT base_url, granule_id 
		FROM`+" `bigquery-public-data.cloud_storage_geo_index.sentinel_2_index` "+
		`WHERE 
			(((south_lat BETWEEN %s AND %s) OR (north_lat BETWEEN %s AND %s)) AND ((west_lon BETWEEN %s AND %s) OR (east_lon BETWEEN %s AND %s))) OR
			((%s BETWEEN south_lat AND north_lat AND %s BETWEEN south_lat AND north_lat) AND (%s BETWEEN west_lon AND east_lon AND %s BETWEEN west_lon AND east_lon)) OR
			((south_lat BETWEEN %s AND %s AND north_lat BETWEEN %s AND %s) AND (west_lon BETWEEN %s AND %s AND east_lon BETWEEN %s AND %s))
		`, lat1, lat2, lat1, lat2, lng1, lng2, lng1, lng2, lat1, lat2, lng1, lng2, lat1, lat2, lat1, lat2, lng1, lng2, lng1, lng2)

	query := client.Query(sql)
	query.QueryConfig.UseStandardSQL = true

	return query.Read(ctx)
}

// Get a handle to a bucket in Google Cloud Storage specified by a bucket-id
func getBucketHandle(bucketID string) (*storage.BucketHandle, error) {

	ctx := context.Background()

	client, err := storage.NewClient(ctx)

	if err != nil {
		return nil, err
	}

	return client.Bucket(bucketID), nil
}
