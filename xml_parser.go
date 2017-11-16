package main

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

type InnerResult struct {
	Value string `xml:",innerxml"`
}

type SpectralInformation struct {
	bands []string `xml:"Product_Image_Characteristics>Spectral_Information_List>Spectral_Information"`
}

func getMetadata(path string) (int, int, int, error) {
	pathMetadata, err := getMetadataPath(path)

	fmt.Println(pathMetadata)

	if err != nil {
		return 0, 0, 0, err
	}

	resp, err := http.Get(pathMetadata)

	if err != nil {
		return 0, 0, 0, err
	}

	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)

	var v SpectralInformation

	err = xml.Unmarshal(data, &v)

	fmt.Println(v.bands)

	if err != nil {
		return 0, 0, 0, nil
	}

	return 0, 0, 0, nil
}

func getMetadataPath(path string) (string, error) {

	ctx := context.Background()

	query := &storage.Query{Prefix: path}

	it := StorageBucket.Objects(ctx, query)

	for {
		objAttrs, err := it.Next()

		if err == iterator.Done {
			break
		}

		if err != nil {
			return "", err
		}

		_path := strings.TrimPrefix(objAttrs.Name, path)

		if !strings.Contains(_path, "/") && strings.HasSuffix(_path, ".xml") && !strings.Contains(_path, "INSPIRE") {
			return objAttrs.MediaLink, nil
		}
	}

	return "", errors.New("Could not find metadata file")
}
