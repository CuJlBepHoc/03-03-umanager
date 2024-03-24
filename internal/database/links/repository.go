package links

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/database"
)

const collection = "links"

func New(db *mongo.Database, timeout time.Duration) *Repository {
	return &Repository{db: db, timeout: timeout}
}

type Repository struct {
	db      *mongo.Database
	timeout time.Duration
}

func (r *Repository) Create(ctx context.Context, req database.CreateLinkReq) (database.Link, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	now := time.Now()

	l := database.Link{
		ID:        req.ID,
		Title:     req.Title,
		URL:       req.URL,
		Images:    req.Images,
		Tags:      req.Tags,
		UserID:    req.UserID,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if _, err := r.db.Collection(collection).InsertOne(ctx, l); err != nil {
		var writeErr mongo.WriteException
		if errors.As(err, &writeErr) && writeErr.HasErrorCode(11000) {
			return l, database.ErrConflict
		}

		return l, fmt.Errorf("mongo InsertOne: %w", err)
	}

	return l, nil
}

func (r *Repository) Update(ctx context.Context, req database.UpdateLinkReq) (database.Link, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	now := time.Now()

	l := database.Link{
		ID:        req.ID,
		Title:     req.Title,
		URL:       req.URL,
		Images:    req.Images,
		Tags:      req.Tags,
		UserID:    req.UserID,
		CreatedAt: now, // обновляем и created_at также. Подумайте как поменять методы так,
		// чтобы поле created_at не обновлялось, это можно сделать разными способоами
		UpdatedAt: now,
	}

	opts := options.Replace().SetUpsert(true)

	if _, err := r.db.Collection(collection).ReplaceOne(ctx, bson.M{"id": req.ID}, l, opts); err != nil {
		return l, fmt.Errorf("mongo ReplaceOne: %w", err)
	}

	return l, nil
}

func (r *Repository) Delete(ctx context.Context, id primitive.ObjectID) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	if _, err := r.db.Collection(collection).DeleteOne(ctx, bson.M{"id": id}); err != nil {
		return fmt.Errorf("mongo DeletOne: %w", err)
	}

	return nil
}

func (r *Repository) FindByID(ctx context.Context, id primitive.ObjectID) (database.Link, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	var l database.Link
	result := r.db.Collection(collection).FindOne(ctx, bson.M{"id": id})
	if err := result.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return l, database.ErrNotFound
		}

		return l, fmt.Errorf("mongo FindOne: %w", err)
	}

	if err := result.Decode(&l); err != nil {
		return l, fmt.Errorf("mongo Decode: %w", err)
	}

	return l, nil
}

func (r *Repository) FindByUserID(ctx context.Context, userID string) ([]database.Link, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	var links []database.Link
	cursor, err := r.db.Collection(collection).Find(ctx, bson.M{"user_id": userID})
	if err != nil {
		return nil, fmt.Errorf("mongo Find: %w", err)
	}

	for cursor.Next(ctx) {
		var l database.Link
		if err := cursor.Decode(&l); err != nil {
			return nil, fmt.Errorf("mongo Decode: %w", err)
		}
		links = append(links, l)
	}
	return links, nil
}

func (r *Repository) FindByUserAndURL(ctx context.Context, link, userID string) (database.Link, error) {
	var l database.Link
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	result := r.db.Collection(collection).FindOne(ctx, bson.M{"url": link, "user_id": userID})
	if err := result.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return l, database.ErrNotFound
		}
		return l, fmt.Errorf("mongo FindOne: %w", err)
	}

	if err := result.Decode(&l); err != nil {
		return l, fmt.Errorf("mongo Decode: %w", err)
	}

	return l, nil
}

func (r *Repository) FindAll(ctx context.Context) ([]database.Link, error) {
	return r.FindByCriteria(ctx, database.FindLinkCriteria{})
}

func (r *Repository) FindByCriteria(ctx context.Context, criteria database.FindLinkCriteria) ([]database.Link, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	var links []database.Link

	filter := bson.M{}
	opts := options.Find()
	if criteria.Limit != nil {
		opts.SetLimit(*criteria.Limit)
	}
	if criteria.Offset != nil {
		opts.SetSkip(*criteria.Offset)
	}
	if criteria.UserID != nil {
		filter["user_id"] = *criteria.UserID
	}
	if len(criteria.Tags) > 0 {
		tagsCriteria := make([]interface{}, 0, len(criteria.Tags))
		for _, tag := range criteria.Tags {
			tagsCriteria = append(tagsCriteria, tag)
		}

		filter["tags"] = bson.M{"$in": tagsCriteria}
	}

	cursor, err := r.db.Collection(collection).Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("mongo Find: %w", err)
	}

	for cursor.Next(ctx) {
		var l database.Link
		if err := cursor.Decode(&l); err != nil {
			return nil, fmt.Errorf("mongo Decode: %w", err)
		}
		links = append(links, l)
	}

	return links, nil
}
