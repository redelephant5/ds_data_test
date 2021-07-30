package locate

import (
	"ds_data_test/lib/rabbitmq"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var objects = make(map[string]int)
var mutex sync.Mutex

type locateMessage struct {
	Addr string
	Id int
}

// 元数据服务
//func Locate (name string) bool {
//	_, err := os.Stat(name)
//	return !os.IsNotExist(err)
//}

// 数据去重版本
//func Locate (hash string) bool {
//	mutex.Lock()
//	_, ok := objects[hash]
//	mutex.Unlock()
//	return ok
//}

func Locate (hash string) int {
	mutex.Lock()
	id, ok := objects[hash]
	mutex.Unlock()
	if !ok {
		return -1
	}
	return id
}

// 数据去重版本
//func Add(hash string){
//	mutex.Lock()
//	objects[hash] = 1
//	mutex.Unlock()
//}
func Add (hash string, id int){
	mutex.Lock()
	objects[hash] = id
	mutex.Unlock()
}


func Del(hash string){
	mutex.Lock()
	delete(objects, hash)
	mutex.Unlock()
}


// 分布式
//func StartLocate() {
//	q := rabbitmq.New(os.Getenv("RABBITMQ_SERVER"))
//	defer q.Close()
//	q.Bind("dataSer")
//	c := q.Consume()
//	for msg := range c {
//		object ,err := strconv.Unquote(string(msg.Body))
//		if err != nil {
//			panic(err)
//		}
//		if Locate(os.Getenv("STORAGE_ROOT") + "/objects/" + object){
//			q.Send(msg.ReplyTo, os.Getenv("LISTEN_ADDRESS"))
//		}
//	}
//}

// 数据去重版本
//func StartLocate(){
//	q := rabbitmq.New(os.Getenv("RABBITMQ_SERVER"))
//	defer q.Close()
//	q.Bind("dataSer")
//	c := q.Consume()
//	for msg := range c{
//		hash, err := strconv.Unquote(string(msg.Body))
//		if err != nil{
//			panic(err)
//		}
//		exist := Locate(hash)
//		if exist {
//			q.Send(msg.ReplyTo, os.Getenv("LISTEN_ADDRESS"))
//		}
//	}
//}
func StartLocate() {
	q := rabbitmq.New(os.Getenv("RABBITMQ_SERVER"))
	defer q.Close()
	q.Bind("dataSer")
	c := q.Consume()
	for msg := range c {
		hash, e := strconv.Unquote(string(msg.Body))
		if e != nil {
			panic(e)
		}
		id := Locate(hash)
		if id != -1 {
			q.Send(msg.ReplyTo, locateMessage{Addr: os.Getenv("LISTEN_ADDRESS"), Id:id})
		}
	}
}

// 数据去重版本
//func CollectObjects() {
//	files, _ := filepath.Glob(os.Getenv("STORAGE_ROOT") + "/objects/*")
//	for i := range files{
//		hash := filepath.Base(files[i])
//		objects[hash] = 1
//	}
//}

func CollectObjects() {
	files, _ := filepath.Glob(os.Getenv("STORAGE_ROOT") + "/objects/*")
	for i := range files {
		file := strings.Split(filepath.Base(files[i]), ".")
		if len(file) != 3{
			panic(files[i])
		}
		hash := file[0]
		id, e := strconv.Atoi(file[1])
		if e != nil {
			panic(e)
		}
		objects[hash] = id
	}
}