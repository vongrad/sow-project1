package main

import (
	"fmt"
	"log"
	"net/http"

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

	ctx := context.Background()

	fmt.Fprint(w, fmt.Sprintf("Lng: %v, Lat: %v", lng, lat))

	query := &storage.Query{Delimiter: "/IMG_DATA", Prefix: "tiles/35/K/KQ/S2A_MSIL1C_20160104T083649_N0201_R078_T35KKQ_20160104T155000.SAFE/GRANULE/"}

	it := StorageBucket.Objects(ctx, query)

	for {
		objAttrs, err := it.Next()

		if err == iterator.Done {
			break
		}

		if err != nil {
			fmt.Println(err.Error())
		}

		fmt.Println(objAttrs.Name)
	}

	// if err != nil {
	// 	fmt.Fprint(w, err.Error())
	// }
}

func getBucketHandle(bucketID string) (*storage.BucketHandle, error) {

	ctx := context.Background()

	client, err := storage.NewClient(ctx)

	if err != nil {
		return nil, err
	}

	return client.Bucket(bucketID), nil
}

// func getImages(path string) (storage.ObjectHandle error) {

// 	rc, err := StorageBucket.Object("tiles/40/R/DS/S2A_MSIL1C_20170921T064621_N0205_R020_T40RDS_20170921T065933.SAFE")

// 	if err != nil {
// 		d.errorf("readFile: unable to open file from bucket %q, file %q: %v", d.bucketName, fileName, err)
// 		return
// 	}
// }

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
