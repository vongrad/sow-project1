package main

import (
	"fmt"
	"log"
	"net/http"
	"path"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/gorilla/mux"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
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

	lng := r.FormValue("lng")
	lat := r.FormValue("lat")

	links := make([]string, 0)

	dbit, err := getBaseURLs(lng, lat)

	if err != nil {
		fmt.Println(err.Error())
	}

	for {
		var row []bigquery.Value

		err := dbit.Next(&row)

		if err == iterator.Done {
			break
		}

		if err != nil {
			fmt.Println(err.Error())
		}

		path := strings.TrimPrefix(row[0].(string), "gs://gcp-public-data-sentinel-2/") + "/GRANULE/"

		_links, err := getImages(path)

		links = append(links, _links...)

		if err != nil {
			fmt.Println(err.Error())
		}
	}

	fmt.Fprint(w, links)
}

func getImages(_path string) ([]string, error) {

	ctx := context.Background()
	links := make([]string, 0)

	//query := &storage.Query{Prefix: "tiles/35/K/KQ/S2A_MSIL1C_20160104T083649_N0201_R078_T35KKQ_20160104T155000.SAFE/GRANULE/"}
	query := &storage.Query{Prefix: _path}

	it := StorageBucket.Objects(ctx, query)

	for {
		objAttrs, err := it.Next()

		if err == iterator.Done {
			break
		}

		if err != nil {
			fmt.Println(err.Error())
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

	fmt.Println(lat, lng)

	client, err := bigquery.NewClient(ctx, "avon-178408")

	if err != nil {
		return nil, err
	}

	sql := fmt.Sprintf(`SELECT base_url 
		FROM`+" `bigquery-public-data.cloud_storage_geo_index.sentinel_2_index` "+
		`WHERE south_lat <= %s AND north_lat >= %s AND west_lon <= %s AND east_lon >= %s;`,
		lat, lat, lng, lng)

	fmt.Println(sql)

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

func registerHandlers() {

	r := mux.NewRouter()

	r.Handle("/image", JSONHandler{getImagesHandler}).Methods("GET")

	http.Handle("/", r)

	http.ListenAndServe("127.0.0.1:8888", r)
}

func main() {
	var err error

	StorageBucketName = "gcp-public-data-sentinel-2"
	StorageBucket, err = getBucketHandle(StorageBucketName)

	if err != nil {
		log.Fatal(err.Error())
	}

	registerHandlers()
}
