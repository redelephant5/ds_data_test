package objects

import (
	"compress/gzip"
	"crypto/sha256"
	"ds_data_test/lib/locate"
	"encoding/base64"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func Handler (w http.ResponseWriter, r *http.Request) {
	m := r.Method
	//if m == http.MethodPut {
	//	put(w, r)
	//	return
	//}
	if m == http.MethodGet {
		get(w, r)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

// 元数据服务版本
//func put(w http.ResponseWriter, r *http.Request){
//	f, err := os.Create(os.Getenv("STORAGE_ROOT") + "/objects/" + strings.Split(r.URL.EscapedPath(), "/")[2])
//	if err != nil {
//		log.Println(err)
//		w.WriteHeader(http.StatusInternalServerError)
//		return
//	}
//	defer f.Close()
//	io.Copy(f, r.Body)
//}

// 元数据服务版本
//func get(w http.ResponseWriter, r *http.Request){
//	f, err := os.Open(os.Getenv("STORAGE_ROOT") + "/objects/" + strings.Split(r.URL.EscapedPath(), "/")[2])
//	if err != nil {
//		log.Println(err)
//		w.WriteHeader(http.StatusNotFound)
//		return
//	}
//	defer f.Close()
//	io.Copy(w, f)
//}

func get(w http.ResponseWriter, r *http.Request){
	file := getFile(strings.Split(r.URL.EscapedPath(), "/")[2])
	if file == ""{
		w.WriteHeader(http.StatusNotFound)
		return
	}
	sendFile(w, file)
}

// 数据去重版本
//func getFile(hash string) string {
//	file := os.Getenv("STORAGE_ROOT") + "/objects/" + hash
//	f, _ := os.Open(file)
//	d := url.PathEscape(utils.CalculateHash(f)) //数据降解
//	f.Close()
//	if d != hash{
//		log.Println("object hash mismatch, remove", file)
//		locate.Del(hash)
//		os.Remove(file)
//		return ""
//	}
//	return file
//}

func getFile(name string) string {
	files, _ := filepath.Glob(os.Getenv("STORAGE_ROOT") + "/objects/" + name + ".*")
	if len(files) != 1 {
		return ""
	}
	file := files[0]
	h := sha256.New()
	sendFile(h, file)
	d := url.PathEscape(base64.StdEncoding.EncodeToString(h.Sum(nil)))
	hash := strings.Split(file, ".")[2]
	if d != hash {
		log.Println("object hash mismatch, remove", file)
		locate.Del(hash)
		os.Remove(file)
		return ""
	}
	return file
}

// 断点续传版本
//func sendFile(w io.Writer, file string){
//	f, _ := os.Open(file)
//	defer f.Close()
//	io.Copy(w, f)
//}

func sendFile(w io.Writer, file string){
	f, e := os.Open(file)
	if e != nil {
		log.Println(e)
		return
	}
	defer f.Close()
	gzipStream, e := gzip.NewReader(f)
	if e != nil {
		log.Println(e)
		return
	}
	io.Copy(w, gzipStream)
	gzipStream.Close()
}