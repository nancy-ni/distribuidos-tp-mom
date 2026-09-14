package common

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func ProcessDelivery(d amqp.Delivery, callbackFunc func(msg m.Message, ack func(), nack func())) {
	msg := m.Message{Body: string(d.Body)}

	ack := func() { d.Ack(false) }
	nack := func() { d.Nack(false, true) }

	callbackFunc(msg, ack, nack)
}

func CloseAllResources(conn *amqp.Connection, sendChannel *amqp.Channel, recvChannel *amqp.Channel) error {
	err := sendChannel.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	err = recvChannel.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	err = conn.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
