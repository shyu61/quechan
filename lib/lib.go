package lib

import (
	"errors"
	"fmt"
	"log"
)

var queue = make(map[string][]string)

type Client struct {
	Namespace string
}

type Topic struct {
	Name string
}

type Message struct {
	Data []byte
}

type Response struct {
	Body string
	Code uint32
}

func NewClient(n string) (*Client, error) {
	if n == "" {
		err := errors.New("Invalid namespace")
		return nil, err
	}
	fmt.Printf("Register namespace=%s\n", n)
	client := Client{Namespace: n}
	return &client, nil
}

func (c Client) CreateTopic(t string) (*Topic, error) {
	if t == "" {
		err := errors.New("Invalid topic name")
		return nil, err
	}
	fmt.Printf("Register topic=%s\n", t)
	topic := Topic{Name: t}
	return &topic, nil
}

func (t Topic) Publish(m *Message) Response {
	queue[t.Name] = append(queue[t.Name], string(m.Data))
	log.Printf("%s: %v\n", t.Name, queue[t.Name]) // all messages in the queue
	res := Response{Body: "Ok", Code: 200}
	return res
}
