package factory

import (
	"strconv"

	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/queue"
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

	return queue.NewQueueMiddleware(conn, sendChannel, recvChannel, queueName), nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return nil, nil
}
