package database

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Link struct {
	ID        primitive.ObjectID `json:"id" bson:"_id"`
	Title     string             `json:"title" bson:"title,omitempty"`
	URL       string             `json:"url" bson:"url"`
	Images    []string           `json:"images,omitempty" bson:"images"`
	Tags      []string           `json:"tags,omitempty" bson:"tags"`
	UserID    string             `json:"user_id" bson:"user_id"`
	CreatedAt time.Time          `json:"created_at" bson:"created_at"`
	UpdatedAt time.Time          `json:"updated_at" bson:"updated_at"`
}

type CreateLinkReq struct {
	ID     primitive.ObjectID `json:"id"`
	URL    string             `json:"url"`
	Title  string             `json:"title"`
	Tags   []string           `json:"tags,omitempty"`
	Images []string           `json:"images,omitempty"`
	UserID string             `json:"user_id"`
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
