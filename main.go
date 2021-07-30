package main

import (
	"ds_data_test/lib/heartbeat"
	"ds_data_test/lib/locate"
	"ds_data_test/objects"
	"ds_data_test/temp"
	"log"
	"net/http"
	"os"
)


func main()  {
	locate.CollectObjects()
	go heartbeat.StartHeartbeat()
	go locate.StartLocate()
	http.HandleFunc("/objects/", objects.Handler)
	http.HandleFunc("/temp/", temp.Handler)
	log.Fatal(http.ListenAndServe(os.Getenv("LISTEN_ADDRESS"), nil))

}



