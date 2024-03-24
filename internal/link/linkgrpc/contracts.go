package linkgrpc

import (
	"context"

	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/database"
)

type linksRepository interface {
	Create(ctx context.Context, req database.CreateLinkReq) (database.Link, error)
	Update(ctx context.Context, req database.UpdateLinkReq) (database.Link, error)
	Delete(ctx context.Context, id primitive.ObjectID) error
	FindByID(ctx context.Context, id primitive.ObjectID) (database.Link, error)
	FindByUserID(ctx context.Context, userID string) ([]database.Link, error)
	FindAll(ctx context.Context) ([]database.Link, error)
}

type amqpPublisher interface {
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
}
