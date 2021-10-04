// namespaceが一意でない時や上限数を超えている場合、errを返す
client, err := queuechan.NewClient(namespace)

// topic名が一意でない時や上限数を超えている場合、errを返す
topic, err := client.CreateTopic("topic-name")

res := topic.Publish(&queuechan.Message{Data: []byte("payload"))

// subscriber名が一意でない時や、上限数を超えている時、指定したtopicが登録されていない場合、errを返す
sub, err := client.CreateSubscription("sub-name", pubsub.SubscriptionConfig{Topic: "topic-name"})

// Ack()または、Nack()をコールする必要がある. しなければ再送される.
err := sub.Receive(func(m *Message) {
	m.Ack() // or m.Nack()
}
