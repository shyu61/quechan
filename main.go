package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	queuechan "shyu61/quechan/lib"
)

type PublishRequest struct {
	Topic   string `json:"topic"`
	Message string `json:"message"`
}

type SubscribeRequest struct {
	Topic string `json:"topic"`
}

var queue = make(map[string][]string)
var max_queue_size = 10000

// POST /publish
func publisher(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprint(w, "Invalid http method")
		return
	}

	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}

	var req PublishRequest
	if err := json.Unmarshal(body, &req); err != nil {
		log.Fatal(err)
	}

	if len(queue[req.Topic]) > max_queue_size {
		log.Fatal("Over max queue size")
		return
	}
	queue[req.Topic] = append(queue[req.Topic], req.Message)
	fmt.Fprintf(w, "Enqued: %v", queue)
}

// POST /subscribe
func subscriber(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprint(w, "Invalid http method")
		return
	}

	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}

	var req SubscribeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		log.Fatal(err)
	}

	fmt.Fprintf(w, "Dequed: %s", queue[req.Topic][0])
	queue[req.Topic] = queue[req.Topic][1:]
}

func main() {
	// create client
	client, err := queuechan.NewClient("foobar")
	if err != nil {
		log.Fatal(err)
	}
	topic, err := client.CreateTopic("topic-name")
	if err != nil {
		log.Fatal(err)
	}
	res := topic.Publish(&queuechan.Message{Data: []byte("payload")})
	if res.Code != 200 {
		log.Printf("Code=%d, Body=%s", res.Code, res.Body)
	}

	http.HandleFunc("/publish", publisher)
	http.HandleFunc("/subscribe", subscriber)

	http.ListenAndServe(":8080", nil)
}
