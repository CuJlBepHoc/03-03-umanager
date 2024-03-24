package link_updater

import (
	"context"
)

func New(repository repository, consumer amqpConsumer) *Story {
	return &Story{repository: repository, consumer: consumer}
}

type Story struct {
	repository repository
	consumer   amqpConsumer
}

func (s *Story) Run(ctx context.Context) error {
	// implement me

	// Слушаем очередь и вызываем пакет scrape
	// Сообщение которое получаем с rabbitmq
	type message struct {
		ID string `json:"id"`
	}
	// Получаем текущий объект ссылки
	// Добавляем данные из scrape
	// Обновляем данные в DB
	return nil
}
