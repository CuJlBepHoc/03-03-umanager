package usergrpc

import (
	"context"

	"github.com/google/uuid"

	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/database"
)

type usersRepository interface {
	Create(ctx context.Context, req database.CreateUserReq) (database.User, error)
	FindByID(ctx context.Context, userID uuid.UUID) (database.User, error)
	DeleteByUserID(ctx context.Context, userID uuid.UUID) error
	FindAll(ctx context.Context) ([]database.User, error)
}
