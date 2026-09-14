package factory

import (
	common "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/common"
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
	consumerTag := common.GenerateNewConsumerTag(exchange)
	return &ExchangeMiddleware{
		conn:        conn,
		sendChannel: sendChannel,
		recvChannel: recvChannel,
		exchange:    exchange,
		queueName:   queueName,
		consumerTag: consumerTag,
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
		common.ProcessDelivery(d, callbackFunc)
	}
	return nil
}

func (em *ExchangeMiddleware) StopConsuming() error {
	err := em.recvChannel.Cancel(em.consumerTag, false)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (em *ExchangeMiddleware) Close() error {
	return common.CloseAllResources(em.conn, em.sendChannel, em.recvChannel)
}
