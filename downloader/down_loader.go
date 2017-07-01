package main


import (
	"github.com/heroku/docker-registry-client/registry"
	"github.com/docker/distribution/digest"
	manifestV2 "github.com/docker/distribution/manifest/schema2"
	//"github.com/docker/distribution/manifest"
	//"github.com/docker/libtrust"
	"fmt"
	//"C"
	//"io"
	//"net/http"
	"io/ioutil"
	"bytes"
	"io"
	"os"
	"flag"
	"net/http"
	"strings"
	"time"
)
/* TODO
	1. add log file
	2. fix the parameter names' more readable
	3. catch exceptions (input, timeout)
*/

func registry_init() (*registry.Registry, error){
	url      := "https://registry-1.docker.io/"
	username := "" // anonymous
	password := "" // anonymous
	hub, err := registry.New(url, username, password)
	if err != nil{
		//
	}
	//repositories, err := hub.Repositories()
	return hub, err
}

func docker_download(input_op string, input_repo string, input_tag string, input_filename string) (*manifestV2.DeserializedManifest) {
	hub, _ := registry_init()

	repo_name := input_repo
	repo_op := input_op
	repo_tag := input_tag
	absfilename := input_filename

	switch repo_op {
	case "download_blobs":
		get_blobs(hub, repo_name, repo_tag, absfilename)
	case "download_manifest":
		get_manifest(hub, repo_name, repo_tag, absfilename)
		//printResponse(manifest)
	}

	return nil
}

func get_manifest(hub *registry.Registry, repo_name string, repo_tag string, absfilename string) error {
	//fmt.Printf("%v\n", repositories)
	//tags, err := hub.Tags("library/ubuntu")
	//fmt.Printf("%v\n", tags)
	//manifest, err := hub.Manifest("library/ubuntu", "1")
	//fmt.Printf("%v\n", manifest)
	registry.Log("start get_manifest")
	manifest2, err := hub.ManifestV2(repo_name, repo_tag)
	//fmt.Printf("%v\n", manifest2)
	printResponse(manifest2)
	storeBlob(absfilename, manifest2.Body)
	defer manifest2.Body.Close()

	return err
}

func get_blobs(hub *registry.Registry, repo_name string, blob_digest string, absfilename string) error {
	digest := digest.NewDigestFromHex(
		"sha256",
		blob_digest,
	)
	reader, err := hub.DownloadLayer(repo_name, digest)
	if reader != nil {
		defer reader.Close()
	}
	if err != nil {
		return err
	}

	storeBlob(absfilename, reader)
	defer reader.Close()
	return nil
}

func printResponse(resp *http.Response) {
	var response []string

	bs, err := ioutil.ReadAll(resp.Body)
	if err != nil{
		//	return  nil
	}
	rdr1 := ioutil.NopCloser(bytes.NewBuffer(bs))
	rdr2 := ioutil.NopCloser(bytes.NewBuffer(bs))
	//doStuff(rdr1)
	resp.Body = rdr2

	buf1 := new(bytes.Buffer)
	buf1.ReadFrom(rdr1)
	tr := buf1.String()

	//tr := string(rdr1)
	//buf1 := new(bytes.Buffer)
	//buf1.ReadFrom(resp.Body)
	//bs1 := buf1.String()

	response = append(response, fmt.Sprintf("%v", tr))

	//// Loop through headers
	//for name, headers := range resp.Header {
	//	name = strings.ToLower(name)
	//	for _, h := range headers {
	//		response = append(response, fmt.Sprintf("%v: %v", name, h))
	//	}
	//}

	//logrus.Debugf("PingV2Registry: http.NewRequest: GET %s body:nil", endpointStr)

	strings.Join(response, "\n")
	registry.Log("<manifest>%s<manifest>\n", response)
}

func storeBlob(absFileName string, resp io.ReadCloser) error {

	registry.Log("start storeBlob")

	//bs, err := ioutil.ReadAll(resp)
	//if err != nil{
	//	//return nil
	//}
	//rdr1 := ioutil.NopCloser(bytes.NewBuffer(bs))
	//rdr2 := ioutil.NopCloser(bytes.NewBuffer(bs))
	//resp = rdr2
	//
	//buf1 := new(bytes.Buffer)
	//buf1.ReadFrom(rdr1)
	//
	//err = ioutil.WriteFile(absFileName, buf1.Bytes(), 0644)
	//if err != nil {
	//	//err handling
	//}
	/*
	Given an io.ReadCloser, from the response of an HTTP request for example,
	what is the most efficient way both in memory overhead and code readability
	to stream the response to a File?
	*/
	start := time.Now().UnixNano()
	outFile, _ := os.Create(absFileName)
	defer  outFile.Close()
	size, err := io.Copy(outFile, resp)
	if err != nil{

	}
	end := time.Now().UnixNano()
	elapsed := float64((end - start) / 1000) //millisecond
	registry.Log("finished storeBlob time: ====> (%v MB / %v s) %v MB/s", float64(size) / 1024 / 1024,
		float64(elapsed) / 1000000,
		float64(size) / float64(elapsed))
	return nil
}

func main() {

	input_op := flag.String("operation", "download_blobs", "download_blobs or download_manifest")
	input_filename := flag.String("filename", "xdn", "The input file which contains all the images'names")
	input_dir := flag.String("dirname", "/gpfs/docker", "The output directory which will contain three directories: manifests, configs, and layers")
	input_tag := flag.String("tag", "latest", "repo tag or layer digest")

	flag.Parse()
	fmt.Println("filename:", *input_filename)
	fmt.Println("dirname:", *input_dir)
	fmt.Println("operation:", *input_op)
	fmt.Println("tag:", *input_tag)

	docker_download(*input_op, *input_filename, *input_tag, *input_dir)

	//open the file
	//if file, err := os.Open()
}


