package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	conn        *amqp.Connection
	sendChannel *amqp.Channel
	recvChannel *amqp.Channel
	exchange    string
	queueName   string
	consumerTag string
	keys        []string
}

func NewExchangeMiddleware(conn *amqp.Connection, sendChannel *amqp.Channel, recvChannel *amqp.Channel, exchange string, queueName string, keys []string) *ExchangeMiddleware {
	return &ExchangeMiddleware{
		conn:        conn,
		sendChannel: sendChannel,
		recvChannel: recvChannel,
		exchange:    exchange,
		queueName:   queueName,
		keys:        keys,
	}
}

func (em *ExchangeMiddleware) Send(msg m.Message) error {
	body := []byte(msg.Body)

	for _, routingKey := range em.keys {
		err := em.sendChannel.Publish(
			em.exchange,
			routingKey,
			false, // mandatory
			false, // inmediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
			},
		)
		if err != nil {
			if em.sendChannel.IsClosed() {
				return m.ErrMessageMiddlewareDisconnected
			}
			return m.ErrMessageMiddlewareMessage
		}
	}
	return nil
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	msgs, err := em.recvChannel.Consume(
		em.queueName,
		"",    // consumerTag
		false, // autoack
		false, // exclusive
		false, // nolocal
		false, // nowait
		nil,   // args
	)
	if err != nil {
		if em.recvChannel.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	for d := range msgs {
		if em.consumerTag == "" {
			em.consumerTag = d.ConsumerTag
		}
		msg := m.Message{Body: string(d.Body)}

		ack := func() { d.Ack(false) }
		nack := func() { d.Nack(false, true) }

		callbackFunc(msg, ack, nack)
	}
	return nil
}

func (em *ExchangeMiddleware) StopConsuming() error {
	if em.consumerTag == "" {
		return m.ErrMessageMiddlewareClose
	}
	err := em.recvChannel.Cancel(em.consumerTag, false)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (em *ExchangeMiddleware) Close() error {
	err := em.sendChannel.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	err = em.recvChannel.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	err = em.conn.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
