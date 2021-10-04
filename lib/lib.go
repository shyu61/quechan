package lib

import (
	"errors"
	"fmt"
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
	Topic Topic
	// Config SubscriptionConfig
}

type Message struct {
	Data []byte
	Topic Topic
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
	// 重複確認
	_, ok := pool[t]
	if ok {
		err := errors.New("Can't use topic name")
		return nil, err
	}
	fmt.Printf("Register topic=%s\n", t)
	topic := Topic{Name: t}
	pool[t] = []string{}
	return &topic, nil
}

func (c Client) CreateSubscription(s string, t Topic) (*Subscription, error) {
	if s == "" {
		err := errors.New("Invalid subscription name")
		return nil, err
	}
	_, ok := pool[t.Name]
	if !ok {
		err := errors.New("Topic not found")
		return nil, err
	}
	fmt.Printf("Register subscription=%s\n", s)
	sub := Subscription{Name: s, Topic: t}
	return &sub, nil
}

func (t Topic) Publish(m *Message) Response {
	_, ok := pool[t.Name]
	if !ok {
		res := Response{Body: "topic not found", Code: 404}
		return res
	}
	pool[t.Name] = append(pool[t.Name], string(m.Data))
	fmt.Printf("%s: %v\n", t.Name, pool[t.Name]) // all messages in the pool
	res := Response{Body: "Ok", Code: 200}
	return res
}

func (s Subscription) Receive(fn func(m *Message)) error {
	topic := s.Topic
	if len(pool[topic.Name]) == 0 {
		err := errors.New("Queue is emply")
		return err
	}
	m := pool[topic.Name][0]
	pool[topic.Name] = pool[topic.Name][1:]
	fmt.Printf("Dequed message=%s", m)

	message := Message{Data: []byte(m), Topic: topic}
	fn(&message) // ここで内部的にAck() or Nack()が呼び出される

	err := errors.New("Something went wront")
	return err
}

func (m Message) Ack() {
}

func (m Message) Nack() {
}
