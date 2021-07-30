package heartbeat

import (
	"ds_data_test/lib/rabbitmq"
	"os"
	"time"
)

func StartHeartbeat(){
	q := rabbitmq.New(os.Getenv("RABBITMQ_SERVER"))
	defer q.Close()
	for {
		q.Publish("apiSer", os.Getenv("LISTEN_ADDRESS"))
		time.Sleep(5*time.Second)
	}
}
