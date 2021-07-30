package temp

import (
	"compress/gzip"
	"ds_data_test/lib/locate"
	"ds_data_test/lib/utils"
	"encoding/json"
	"fmt"
	"github.com/satori/go.uuid"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type tempInfo struct {
	Uuid string
	Name string
	Size int64
}

func (t *tempInfo) hash() string {
	s := strings.Split(t.Name, ".")
	return s[0]
}

func (t *tempInfo) id() int {
	s := strings.Split(t.Name, ".")
	id, _ := strconv.Atoi(s[1])
	return id
}

func Handler(w http.ResponseWriter, r *http.Request){
	m := r.Method
	if m == http.MethodHead{
		head(w, r)
		return
	}
	if m == http.MethodGet{
		get(w, r)
		return
	}
	if m == http.MethodPut{
		put(w, r)
		return
	}
	if m == http.MethodPatch{
		patch(w, r)
		return
	}
	if m == http.MethodPost{
		post(w, r)
		return
	}
	if m == http.MethodDelete{
		del(w, r)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func post(w http.ResponseWriter, r *http.Request){
	var u uuid.UUID
	//output, _ := exec.Command("uuidgen").Output()
	//uuid := strings.TrimSuffix(string(output), "\n")
	u = uuid.NewV4()
	uStr := u.String()
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	size, e := strconv.ParseInt(r.Header.Get("size"), 0, 64)
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	t := tempInfo{uStr, name, size}
	e = t.writeToFile()
	if e != nil{
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// 保存临时对象文件内容
	os.Create(os.Getenv("STORAGE_ROOT") + "/temp/" + t.Uuid + ".dat")
	w.Write([]byte(uStr))
}

// 保存临时对象信息使用
func (t *tempInfo) writeToFile() error {
	f, e := os.Create(os.Getenv("STORAGE_ROOT") + "/temp/" + t.Uuid)
	if e != nil {
		return e
	}
	defer f.Close()
	b, _ := json.Marshal(t)
	f.Write(b)
	return nil
}

func patch(w http.ResponseWriter, r *http.Request){
	uuid := strings.Split(r.URL.EscapedPath(), "/")[2]
	tempinfo, e := readFromFile(uuid)
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	infoFile := os.Getenv("STORAGE_ROOT") + "/temp/" + uuid
	dataFile := infoFile + ".dat"
	f, e := os.OpenFile(dataFile, os.O_WRONLY|os.O_APPEND, 0)
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer f.Close()
	_, e = io.Copy(f, r.Body)
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	info, e := f.Stat()
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	actual := info.Size()
	if actual > tempinfo.Size {
		os.Remove(dataFile)
		os.Remove(infoFile)
		log.Println("actual size", actual, "exceeds", tempinfo.Size)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func readFromFile(uuid string)(*tempInfo, error){
	f, e := os.Open(os.Getenv("STORAGE_ROOT") + "/temp/" + uuid)
	if e != nil{
		return nil, e
	}
	defer f.Close()
	b, _ := ioutil.ReadAll(f)
	var info tempInfo
	json.Unmarshal(b, &info)
	return &info, nil
}

func put(w http.ResponseWriter, r *http.Request){
	uuid := strings.Split(r.URL.EscapedPath(), "/")[2]
	tempinfo, e := readFromFile(uuid)
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	infoFile := os.Getenv("STORAGE_ROOT") + "/temp/" + uuid
	dataFile := infoFile + ".dat"
	f, e := os.Open(dataFile)
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	actual := info.Size()
	os.Remove(infoFile)
	if actual != tempinfo.Size{
		os.Remove(dataFile)
		log.Println("actual size mismatch, expect", tempinfo.Size, "actual", actual)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	commitTempObject(dataFile, tempinfo)
}

// 数据去重版本
//func commitTempObject(dataFile string, tempinfo *tempInfo){
//	os.Rename(dataFile, os.Getenv("STORAGE_ROOT")+"/objects/"+tempinfo.Name)
//	locate.Add(tempinfo.Name)
//}

// 断点续传版本
//func commitTempObject(dataFile string, tempinfo *tempInfo){
//	f, _ := os.Open(dataFile)
//	d := url.PathEscape(utils.CalculateHash(f))
//	f.Close()
//	os.Rename(dataFile, os.Getenv("STORAGE_ROOT")+"/objects/"+tempinfo.Name+"."+d)
//	locate.Add(tempinfo.hash(), tempinfo.id())
//}
func commitTempObject(dataFile string, tempinfo *tempInfo){
	f, _ := os.Open(dataFile)
	defer f.Close()
	d := url.PathEscape(utils.CalculateHash(f))
	f.Seek(0, io.SeekStart)
	w, _ := os.Create(os.Getenv("STORAGE_ROOT") + "/objects/" + tempinfo.Name + "." + d)
	w2 := gzip.NewWriter(w)
	io.Copy(w2, f)
	w2.Close()
	os.Remove(dataFile)
	locate.Add(tempinfo.hash(), tempinfo.id())
}


func del(w http.ResponseWriter, r *http.Request){
	uuid := strings.Split(r.URL.EscapedPath(), "/")[2]
	infoFile := os.Getenv("STORAGE_ROOT") + "/temp/" + uuid
	dataFile := infoFile + ".dat"
	os.Remove(infoFile)
	os.Remove(dataFile)
}

func get(w http.ResponseWriter, r *http.Request) {
	uuid := strings.Split(r.URL.EscapedPath(), "/")[2]
	f, e := os.Open(os.Getenv("STORAGE_ROOT") + "/temp/" + uuid + ".dat")
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer f.Close()
	io.Copy(w, f)
}

func head(w http.ResponseWriter, r *http.Request) {
	uuid := strings.Split(r.URL.EscapedPath(), "/")[2]
	f, e := os.Open(os.Getenv("STORAGE_ROOT") + "/temp/" + uuid + ".dat")
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("content-length", fmt.Sprintf("%d", info.Size()))
}