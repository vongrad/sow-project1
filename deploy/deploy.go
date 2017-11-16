package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/gorilla/mux"
	"github.com/vongrad/geocoding"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/appengine"
)

var (
	StorageBucket     *storage.BucketHandle
	StorageBucketName string
)

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

	links := make([]string, 0)

	dbit, err := getBaseURLs(lng, lat)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	for {
		var row []bigquery.Value

		err := dbit.Next(&row)

		if err == iterator.Done {
			break
		}

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		path := strings.TrimPrefix(row[0].(string), "gs://gcp-public-data-sentinel-2/") + "/GRANULE/"

		_links, err := getImages(path)

		links = append(links, _links...)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
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
			fmt.Println(objAttrs.MediaLink)
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

	sql := fmt.Sprintf(`SELECT base_url 
		FROM`+" `bigquery-public-data.cloud_storage_geo_index.sentinel_2_index` "+
		`WHERE south_lat <= %s AND north_lat >= %s AND west_lon <= %s AND east_lon >= %s LIMIT 1;`,
		lat, lat, lng, lng)

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

func main() {

	var err error

	StorageBucketName = "gcp-public-data-sentinel-2"
	StorageBucket, err = getBucketHandle(StorageBucketName)

	if err != nil {
		log.Fatal(err.Error())
	}

	r := mux.NewRouter()

	r.Handle("/image", JSONHandler{getImagesHandler}).Methods("GET")

	http.Handle("/", r)

	appengine.Main()
}
