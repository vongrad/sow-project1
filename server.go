package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"net/http/pprof"

	"cloud.google.com/go/storage"
	"github.com/gorilla/mux"
	"github.com/vongrad/geocoding"
)

var (
	// StorageBucket is a reference to currently used Google Cloud Storage bucket
	StorageBucket *storage.BucketHandle
	// StorageBucketName refers to the name of currently used Google Cloud Storage bucket
	StorageBucketName string
)

// GeoFabricHost is an endpoint for fetching country polygons
const GeoFabricHost = "http://download.geofabrik.de/"

// JSONHandler is a generic handler used for setting the 'Content-Type' header to JSON
type JSONHandler struct {
	h http.HandlerFunc
}

func (j JSONHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	j.h(w, r)
}

// Handler used to fetch images for all granules that intersect with a single geo coordinate
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

// Handler used to fetch images of all granules within a geo bound
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

// Handler used to fetch images of all granules for a specific country
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

	count, err := getPolygonImages(points)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type Count struct {
		Count int
	}

	enc := json.NewEncoder(w)
	err = enc.Encode(Count{Count: count})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Attach profiler for to the router for easy profiling
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
