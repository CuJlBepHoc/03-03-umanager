package database

import (
	"time"

	"github.com/google/uuid"
)

type User struct {
	ID        uuid.UUID `db:"id"`
	Username  string    `db:"username"`
	Password  string    `db:"password"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

type CreateUserReq struct {
	ID       uuid.UUID
	Username string
	Password string
}

type FindUserCriteria struct {
	ID       *uuid.UUID
	Username *string
}
