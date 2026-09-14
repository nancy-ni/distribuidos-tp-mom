package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	conn        *amqp.Connection
	sendChannel *amqp.Channel
	recvChannel *amqp.Channel
	queueName   string
	consumerTag string
}

func NewQueueMiddleware(conn *amqp.Connection, sendChannel *amqp.Channel, recvChannel *amqp.Channel, queueName string) *QueueMiddleware {
	consumerTag := generateNewConsumerTag(queueName)
	return &QueueMiddleware{
		conn:        conn,
		sendChannel: sendChannel,
		recvChannel: recvChannel,
		queueName:   queueName,
		consumerTag: consumerTag,
	}
}

func (qm *QueueMiddleware) Send(msg m.Message) error {
	body := []byte(msg.Body)

	err := qm.sendChannel.Publish(
		"",
		qm.queueName,
		false, // mandatory
		false, // inmediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		if qm.sendChannel.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}
	return nil
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	msgs, err := qm.recvChannel.Consume(
		qm.queueName,
		"",    // consumerTag
		false, // autoack
		false, // exclusive
		false, // nolocal
		false, // nowait
		nil,   // args
	)
	if err != nil {
		if qm.recvChannel.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	for d := range msgs {
		msg := m.Message{Body: string(d.Body)}

		ack := func() { d.Ack(false) }
		nack := func() { d.Nack(false, true) }

		callbackFunc(msg, ack, nack)
	}
	return nil
}

func (qm *QueueMiddleware) StopConsuming() error {
	err := qm.recvChannel.Cancel(qm.consumerTag, false)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (qm *QueueMiddleware) Close() error {
	err := qm.sendChannel.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	err = qm.recvChannel.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	err = qm.conn.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
