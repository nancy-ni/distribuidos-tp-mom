package queue

import (
	"sync"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	conn        *amqp.Connection
	sendChannel *amqp.Channel
	recvChannel *amqp.Channel
	queueName   string
	isConsuming bool
	lock        sync.Mutex
}

func NewQueueMiddleware(conn *amqp.Connection, sendChannel *amqp.Channel, recvChannel *amqp.Channel, queueName string) *QueueMiddleware {
	return &QueueMiddleware{
		conn:        conn,
		sendChannel: sendChannel,
		recvChannel: recvChannel,
		queueName:   queueName,
		isConsuming: false,
	}
}

func (qm *QueueMiddleware) Send(msg m.Message) error {
	body := []byte(msg.Body)

	err := qm.sendChannel.Publish(
		"",
		qm.queueName,
		false,
		false,
		amqp.Publishing{ContentType: "application/json", Body: body},
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
	qm.lock.Lock()
	defer qm.lock.Unlock()

	if qm.isConsuming {
		return m.ErrMessageMiddlewareMessage
	}

	consumerTag := qm.getConsumerTag()
	msgs, err := qm.recvChannel.Consume(
		qm.queueName,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		if qm.recvChannel.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	qm.isConsuming = true

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
	qm.lock.Lock()
	defer qm.lock.Unlock()

	if !qm.isConsuming {
		return nil
	}

	consumerTag := qm.getConsumerTag()
	err := qm.recvChannel.Cancel(consumerTag, false)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}

	qm.isConsuming = false
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

func (qm *QueueMiddleware) getConsumerTag() string {
	return "consumer-tag-" + qm.queueName
}
