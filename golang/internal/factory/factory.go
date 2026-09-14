package factory

import (
	"strconv"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	conn, err := amqp.Dial("amqp://" + connectionSettings.Hostname + ":" + strconv.Itoa(connectionSettings.Port))
	if err != nil {
		return nil, err
	}
	sendChannel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	recvChannel, err := conn.Channel()
	if err != nil {
		sendChannel.Close()
		conn.Close()
		return nil, err
	}

	_, err = sendChannel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		sendChannel.Close()
		recvChannel.Close()
		conn.Close()
		return nil, err
	}

	return NewQueueMiddleware(conn, sendChannel, recvChannel, queueName), nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	conn, err := amqp.Dial("amqp://" + connectionSettings.Hostname + ":" + strconv.Itoa(connectionSettings.Port))
	if err != nil {
		return nil, err
	}
	sendChannel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	recvChannel, err := conn.Channel()
	if err != nil {
		sendChannel.Close()
		conn.Close()
		return nil, err
	}

	err = sendChannel.ExchangeDeclare(
		exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		recvChannel.Close()
		sendChannel.Close()
		conn.Close()
		return nil, err
	}

	q, err := recvChannel.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		recvChannel.Close()
		sendChannel.Close()
		conn.Close()
		return nil, err
	}

	for _, routingKey := range keys {
		err := recvChannel.QueueBind(
			q.Name,
			routingKey,
			exchange,
			false,
			nil,
		)
		if err != nil {
			recvChannel.Close()
			sendChannel.Close()
			conn.Close()
			return nil, err
		}
	}
	return NewExchangeMiddleware(conn, sendChannel, recvChannel, exchange, q.Name, keys), nil
}
