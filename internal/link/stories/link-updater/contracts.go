package link_updater

import (
	"context"

	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/database"
)

type repository interface {
	FindByID(ctx context.Context, id primitive.ObjectID) (database.Link, error)
	Update(ctx context.Context, req database.UpdateLinkReq) (database.Link, error)
}

type amqpConsumer interface {
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (
		<-chan amqp.Delivery,
		error,
	)
}
