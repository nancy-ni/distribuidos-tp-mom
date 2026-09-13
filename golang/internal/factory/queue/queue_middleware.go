package queue

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	queueName string
}

func NewQueueMiddleware(conn *amqp.Connection, channel *amqp.Channel, queueName string) *QueueMiddleware {
	return &QueueMiddleware{
		conn:      conn,
		channel:   channel,
		queueName: queueName,
	}
}

func (qm *QueueMiddleware) Send(msg m.Message) error {
	body := []byte(msg.Body)

	err := qm.channel.Publish(
		"",
		qm.queueName,
		false,
		false,
		amqp.Publishing{ContentType: "application/json", Body: body},
	)
	if err != nil {
		if qm.channel.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}
	return nil
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	consumerTag := qm.getConsumerTag()
	msgs, err := qm.channel.Consume(
		qm.queueName,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		if qm.channel.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	go func() {
		for d := range msgs {
			msg := m.Message{Body: string(d.Body)}

			ack := func() { d.Ack(false) }
			nack := func() { d.Nack(false, true) }

			callbackFunc(msg, ack, nack)
		}
	}()
	return nil
}

func (qm *QueueMiddleware) StopConsuming() error {
	consumerTag := qm.getConsumerTag()
	err := qm.channel.Cancel(consumerTag, false)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (qm *QueueMiddleware) Close() error {
	err := qm.channel.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}

func (qm *QueueMiddleware) getConsumerTag() string {
	return "consumer-tag-" + qm.queueName
}
