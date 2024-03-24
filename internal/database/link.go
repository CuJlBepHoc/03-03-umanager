package database

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Link struct {
	ID        primitive.ObjectID `bson:"id"`
	Title     string             `bson:"title,omitempty"`
	URL       string             `bson:"url"`
	Images    []string           `bson:"images"`
	Tags      []string           `bson:"tags"`
	UserID    string             `bson:"user_id"`
	CreatedAt time.Time          `bson:"created_at"`
	UpdatedAt time.Time          `bson:"updated_at"`
}

type CreateLinkReq struct {
	ID     primitive.ObjectID
	URL    string
	Title  string
	Tags   []string
	Images []string
	UserID string
}

type UpdateLinkReq struct {
	ID     primitive.ObjectID
	URL    string
	Title  string
	Tags   []string
	Images []string
	UserID string
}

type FindLinkCriteria struct {
	UserID *string
	Tags   []string
	Limit  *int64
	Offset *int64
}
