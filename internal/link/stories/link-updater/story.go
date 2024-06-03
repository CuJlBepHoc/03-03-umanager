package link_updater

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/database"
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/pkg/scrape"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
	msgCh, err := s.consumer.Consume(
		"link_queue", // название очереди
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("channel Consume: %w", err)
	}

	type message struct {
		ID string `json:"id"`
	}

	go func() {
		for in := range msgCh {
			// Сообщение которое получаем с rabbitmq
			var m message
			if err := json.Unmarshal(in.Body, &m); err != nil {
				slog.Error("unmarshal message", slog.Any("err", err))
				continue
			}

			objectID, err := primitive.ObjectIDFromHex(m.ID)
			if err != nil {
				slog.Error("convert ID to ObjectID", slog.Any("err", err))
				continue
			}

			// Получаем текущий объект ссылки
			link, err := s.repository.FindByID(ctx, objectID)
			if err != nil {
				slog.Error("get link by ID", slog.Any("err", err))
				continue
			}

			// Вызываем пакет scrape и передаем туда URL ссылки
			scrapedData, err := scrape.Parse(ctx, link.URL)
			if err != nil {
				slog.Error("scrape URL", slog.Any("err", err))
				continue
			}

			// Обновляем объект ссылки новыми данными
			updateReq := database.UpdateLinkReq{
				ID:     objectID,
				Title:  link.Title,
				URL:    link.URL,
				Tags:   link.Tags,
				Images: link.Images,
				UserID: link.UserID,
			}

			if scrapedData.Title != "" {
				updateReq.Title = scrapedData.Title
			}

			// Обновляем данные в DB
			if _, err := s.repository.Update(ctx, updateReq); err != nil {
				slog.Error("update link", slog.Any("err", err))
			}
		}
	}()

	// Ждем завершения контекста
	<-ctx.Done()

	return nil
}
