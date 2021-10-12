package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"shyu61/quechan/database"
)

type NamespaceRequest struct {
	Name string `json:"name"`
}

type TopicRequest struct {
	Topic     string `json:"topic"`
	Namespace string `json:"namespace"`
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
		fmt.Fprintf(w, "Invalid parameter errors=%s", err)
		return
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

func handleCreateTopic(w http.ResponseWriter, r *http.Request) {
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

	var t TopicRequest
	if err := json.Unmarshal(body, &t); err != nil {
		fmt.Fprintf(w, "Invalid parameter errors=%s", err)
		return
	}

	fmt.Printf("topic name=%s, namespace=%s", t.Topic, t.Namespace)

	var count_n int
	result_n := database.DB.QueryRow("select count(*) as count from namespaces where name = ?", t.Namespace)
	if err := result_n.Scan(&count_n); err != nil {
		fmt.Fprintf(w, "Error occured message=%s\n", err)
		return
	}
	if count_n == 0 {
		fmt.Fprintf(w, "Not found namespace")
		return
	}

	var count_t int
	result_t := database.DB.QueryRow("select count(*) from topics where name = ? and namespace_id = (select id from namespaces where name = ?)", t.Topic, t.Namespace)
	if err := result_t.Scan(&count_t); err != nil {
		fmt.Fprintf(w, "Error occured message=%s\n", err)
		return
	}
	// namespace単位でtopic.nameは一意
	if count_t != 0 {
		fmt.Fprint(w, "Cannot use topic name\n")
		return
	}

	database.DB.Exec("insert into topics(name, namespace_id) values(?, (select id from namespaces where name = ?))", t.Topic, t.Namespace)
	fmt.Fprintf(w, "Insert topic name=%s\n", t.Topic)
}

func handlePulish(w http.ResponseWriter, r *http.Request) {
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

	var p PublishRequest
	if err := json.Unmarshal(body, &p); err != nil {
		fmt.Fprintf(w, "Invalid parameter errors=%s", err)
		return
	}

	fmt.Printf("Publish topic=%s, message=%s", p.Topic, p.Message)

	// topicの存在確認(db)
	var count int
	result := database.DB.QueryRow("select count(*) from topics where name = ?", p.Topic)
	if err := result.Scan(&count); err != nil {
		fmt.Fprintf(w, "Error occured message=%s\n", err)
		return
	}
	if count == 0 {
		fmt.Fprintf(w, "Not found topic")
		return
	}

	// topicの存在確認(queue)
	_, ok := queue[p.Topic]
	if !ok {
		fmt.Fprintf(w, "Not found topic")
		fmt.Printf("topic=%s exists in db, but not in queue. Data is mismatched. Please check.", p.Topic)
		return
	}

	// queueの上限確認
	if len(queue[p.Topic]) > max_queue_size {
		fmt.Fprintf(w, "Over max queue size")
		return
	}

	queue[p.Topic] = append(queue[p.Topic], p.Message)
	fmt.Fprintf(w, "Enqued")
}

func handleSubscribe(w http.ResponseWriter, r *http.Request) {
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

	var s SubscribeRequest
	if err := json.Unmarshal(body, &s); err != nil {
		fmt.Fprintf(w, "Invalid parameter errors=%s", err)
		return
	}

	fmt.Printf("Subscribe topic=%s", s.Topic)

	// topicの存在確認
	var count int
	result := database.DB.QueryRow("select count(*) from topics where name = ?", s.Topic)
	if err := result.Scan(&count); err != nil {
		fmt.Fprintf(w, "Error occured message=%s\n", err)
		return
	}
	if count == 0 {
		fmt.Fprintf(w, "Not found topic")
		return
	}

	// queueの存在確認
	if len(queue[s.Topic]) == 0 {
		fmt.Fprintf(w, "Queue is empty")
		return
	}

	fmt.Fprintf(w, "%s", queue[s.Topic][0])
	queue[s.Topic] = queue[s.Topic][1:]
}

func main() {
	database.DB = database.Connect()
	defer database.DB.Close()

	http.HandleFunc("/namespace", handleCreateNamespace)
	http.HandleFunc("/topic", handleCreateTopic)
	http.HandleFunc("/publish", handlePulish)
	http.HandleFunc("/subscribe", handleSubscribe)

	http.ListenAndServe("127.0.0.1:8080", nil)
}
