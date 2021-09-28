package lib

import (
	"errors"
	"fmt"
	"log"
)

var pool = make(map[string][]string) // { "topic-name": ["msg1", "msg2", ...], ... }

type Client struct {
	Namespace string
}

type Topic struct {
	Name string
}

type Subscription struct {
	Name string
	Config SubscriptionConfig
}

type SubscriptionConfig struct {
	Topic string
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
	pool[t] = []string{}
	return &topic, nil
}

func (c Client) CreateSubscription(s string, t SubscriptionConfig) (*Subscription, error) {
	if s == "" {
		err := errors.New("Invalid subscription name")
		return nil, err
	}
	if t.Topic == "" {
		err := errors.New("Invalid topic name")
		return nil, err
	}
	_, ok := pool[t.Topic]
	if !ok {
		err := errors.New("Topic not found")
		return nil, err
	}
	fmt.Printf("Register subscription=%s\n", s)
	sub := Subscription{Name: s}
	return &sub, nil
}

func (t Topic) Publish(m *Message) Response {
	key, ok := pool[t.Name]
	if !ok {
		res := Response{Body: "topic not found", Code: 404}
		return res
	}
	key = append(key, string(m.Data))
	log.Printf("%s: %v\n", t.Name, key) // all messages in the pool
	res := Response{Body: "Ok", Code: 200}
	return res
}

func (s Subscription) Receive(fn func(m *Message)) error {
	topic := s.Config.Topic
	queue := pool[topic][0]
	pool[topic] = pool[topic][1:]
	fmt.Printf("Dequed message=%s", queue)

	message := Message{Data: []byte(queue)}
	fn(&message)

	err := errors.New("Something went wront")
	return err
}

func (m Message) Ack() {
}

func (m Message) Nack() {
}
