package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"shyu61/quechan/database"
	queuechan "shyu61/quechan/lib"
)

type NamespaceRequest struct {
	Name string `json:"name"`
}

type PublishRequest struct {
	Topic   string `json:"topic"`
	Message string `json:"message"`
}

type SubscribeRequest struct {
	Topic string `json:"topic"`
}

var queue = make(map[string][]string)
var max_queue_size = 10000

func handleCreateNamespace(w http.ResponseWriter, r *http.Request) {
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

	var n NamespaceRequest
	if err := json.Unmarshal(body, &n); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("namespace name=%s\n", n.Name)

	var count int
	result := database.DB.QueryRow("select count(*) as count from namespaces where name = ?", n.Name)
	if err := result.Scan(&count); err != nil {
		fmt.Fprintf(w, "Error occured message=%s\n", err)
		return
	}
	if count != 0 {
		fmt.Fprint(w, "Cannot use namespace\n")
		return
	}

	database.DB.Exec("insert into namespaces(name) values(?)", n.Name)
	fmt.Fprintf(w, "Insert namespace name=%s\n", n.Name)
}

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
	database.DB = database.Connect()
	defer database.DB.Close()

	namespace := "sample-namespace"
	topic_name := "sample-topic"
	sub_name := "sample-sub"

	client, err := queuechan.NewClient(namespace)
	if err != nil {
		log.Fatal(err)
	}
	topic, err := client.CreateTopic(topic_name)
	if err != nil {
		log.Fatal(err)
	}
	res := topic.Publish(&queuechan.Message{Data: []byte("payload"), Topic: *topic})
	if res.Code != 200 {
		log.Printf("Code=%d, Body=%s", res.Code, res.Body)
	}
	sub, err := client.CreateSubscription(sub_name, *topic)
	if err != nil {
		log.Fatal(err)
	}
	sub.Receive(func(m *queuechan.Message) {
		fmt.Printf("%s", string(m.Data))
		m.Ack()
	})

	http.HandleFunc("/namespace", handleCreateNamespace)
	http.HandleFunc("/publish", publisher)
	http.HandleFunc("/subscribe", subscriber)

	http.ListenAndServe("127.0.0.1:8080", nil)
}
