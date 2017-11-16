package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"path"
	"strconv"
	"strings"

	"net/http/pprof"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/gorilla/mux"
	"github.com/vongrad/geocoding"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

var (
	StorageBucket     *storage.BucketHandle
	StorageBucketName string
)

// GeoFabricHost  is an endpoint for fetching country polygons
const GeoFabricHost = "http://download.geofabrik.de/"

type JSONHandler struct {
	h http.HandlerFunc
}

func (j JSONHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	j.h(w, r)
}

func getImagesHandler(w http.ResponseWriter, r *http.Request) {

	var lng string
	var lat string

	address := r.FormValue("address")

	if address != "" {
		api := geocoding.API{APIKey: "AIzaSyDnqUXP9FS9Ikk6DABkGnVhAaOnEfpLUKA"}

		latitude, longitude, err := api.Geocode(address)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}

		lat = fmt.Sprintf("%f", latitude)
		lng = fmt.Sprintf("%f", longitude)

	} else {
		lng = r.FormValue("lng")
		lat = r.FormValue("lat")

		if lng == "" || lat == "" {
			http.Error(w, "Not enough parameters to complete the request", http.StatusBadRequest)
			return
		}
	}

	links, err := getImageURLs(lat, lng, "", "")

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	enc := json.NewEncoder(w)
	// Do not escape ampersands ('\0026')
	enc.SetEscapeHTML(false)

	err = enc.Encode(links)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getImages2Handler(w http.ResponseWriter, r *http.Request) {

	var lng1 string
	var lat1 string

	var lng2 string
	var lat2 string

	lng1 = r.FormValue("lng1")
	lat1 = r.FormValue("lat1")

	lng2 = r.FormValue("lng2")
	lat2 = r.FormValue("lat2")

	if lng1 == "" || lat1 == "" || lng2 == "" || lat2 == "" {
		http.Error(w, "Not enough parameters to complete the request", http.StatusBadRequest)
		return
	}

	links, err := getImageURLs(lat1, lng1, lat2, lng2)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	enc := json.NewEncoder(w)
	// Do not escape ampersands ('\0026')
	enc.SetEscapeHTML(false)

	err = enc.Encode(links)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getCountryHandler(w http.ResponseWriter, r *http.Request) {
	country := r.FormValue("country")

	if country == "" {
		http.Error(w, "Not enough parameters to complete the request", http.StatusBadRequest)
		return
	}

	data, err := downloadFile(GeoFabricHost + country + ".poly")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	points, err := parsePolyData(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Println(points[1].x)
}

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

	for {
		var row []bigquery.Value

		err := dbit.Next(&row)

		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		path := strings.TrimPrefix(row[0].(string), "gs://gcp-public-data-sentinel-2/") + "/GRANULE/" + row[1].(string) + "/IMG_DATA/"

		_links, err := getImages(path)

		links = append(links, _links...)

		if err != nil {
			return nil, err
		}
	}
	return links, nil
}

func getImages(_path string) ([]string, error) {

	ctx := context.Background()
	links := make([]string, 0)

	query := &storage.Query{Prefix: _path}

	it := StorageBucket.Objects(ctx, query)

	for {
		objAttrs, err := it.Next()

		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		// Get only .jp2 files
		if path.Ext(objAttrs.Name) == ".jp2" {
			links = append(links, objAttrs.MediaLink)
		}
	}

	return links, nil
}

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

func getBaseURLs2(lng1 string, lat1 string, lng2 string, lat2 string) (*bigquery.RowIterator, error) {

	var err error

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, "avon-178408")

	if err != nil {
		return nil, err
	}

	_lat1, err := strconv.ParseFloat(lat1, 64)
	if err != nil {
		return nil, err
	}
	_lng1, err := strconv.ParseFloat(lng1, 64)
	if err != nil {
		return nil, err
	}
	_lat2, err := strconv.ParseFloat(lat2, 64)
	if err != nil {
		return nil, err
	}
	_lng2, err := strconv.ParseFloat(lng2, 64)
	if err != nil {
		return nil, err
	}

	// Either lng/lat in inside the granule bounds or granule is between two lng/lat bounds
	sql := fmt.Sprintf(`SELECT base_url, granule_id 
		FROM`+" `bigquery-public-data.cloud_storage_geo_index.sentinel_2_index` "+
		`WHERE (south_lat >= %v AND north_lat <= %v AND west_lon >= %v AND east_lon <= %v) OR 
		(south_lat <= %s AND north_lat >= %s AND west_lon <= %s AND east_lon >= %s) OR 
		(south_lat <= %s AND north_lat >= %s AND west_lon <= %s AND east_lon >= %s);`,
		math.Min(_lat1, _lat2), math.Max(_lat1, _lat2), math.Min(_lng1, _lng2), math.Max(_lng1, _lng2),
		lat1, lat1, lng1, lng1,
		lat2, lat2, lng2, lng2)

	query := client.Query(sql)

	query.QueryConfig.UseStandardSQL = true

	return query.Read(ctx)
}

func getBucketHandle(bucketID string) (*storage.BucketHandle, error) {

	ctx := context.Background()

	client, err := storage.NewClient(ctx)

	if err != nil {
		return nil, err
	}

	return client.Bucket(bucketID), nil
}

func attachProfiler(router *mux.Router) {
	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
}

func main() {

	var err error

	StorageBucketName = "gcp-public-data-sentinel-2"
	StorageBucket, err = getBucketHandle(StorageBucketName)

	if err != nil {
		log.Fatal(err.Error())
	}

	r := mux.NewRouter()

	attachProfiler(r)

	r.Handle("/image", JSONHandler{getImagesHandler}).Methods("GET")
	r.Handle("/image/2", JSONHandler{getImages2Handler}).Methods("GET")
	r.Handle("/image/country", JSONHandler{getCountryHandler}).Methods("GET")

	http.Handle("/", r)

	http.ListenAndServe("127.0.0.1:8888", r)
}
