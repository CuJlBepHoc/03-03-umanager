package links

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-playground/assert/v2"
	"github.com/google/uuid"
	"github.com/labstack/gommon/log"
	"github.com/sethvargo/go-envconfig"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/database"
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/env/config"
)

var (
	linksRepo *Repository
	client    *mongo.Client
	once      sync.Once
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	once.Do(
		func() {
			var cfg config.Config
			if err := envconfig.Process(ctx, &cfg); err != nil { //nolint:typecheck
				log.Fatalf("env processing: %v", err)
			}

			linksDBConn, err := mongo.Connect(
				ctx, &options.ClientOptions{
					ConnectTimeout: &cfg.LinksService.Mongo.ConnectTimeout,
					Hosts: []string{
						fmt.Sprintf(
							"%s:%d", cfg.LinksService.Mongo.Host, cfg.LinksService.Mongo.Port,
						),
					},
					MaxPoolSize: &cfg.LinksService.Mongo.MaxPoolSize,
					MinPoolSize: &cfg.LinksService.Mongo.MinPoolSize,
				},
			)
			if err != nil {
				log.Fatalf("mongo.Connect: %w", err)
			}

			client = linksDBConn

			linksRepo = New(linksDBConn.Database("links"), 5*time.Second)
		},
	)
	defer func() {
		_ = client.Disconnect(ctx)
	}()

	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestRepository_Create(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip()
	}

	ctx := context.Background()

	id := primitive.NewObjectID()
	_, err := linksRepo.Create(
		ctx, database.CreateLinkReq{
			ID:     id,
			URL:    "https://ya.ru",
			Title:  "ya",
			UserID: uuid.New().String(),
		},
	)
	require.NoError(t, err)

	_, err = linksRepo.FindByID(ctx, id)
	require.NoError(t, err)
}

func TestRepository_Update(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip()
	}

	ctx := context.Background()

	id := primitive.NewObjectID()
	userID := uuid.New().String()
	_, err := linksRepo.Create(
		ctx, database.CreateLinkReq{
			ID:     id,
			URL:    "https://ya.ru",
			Title:  "ya",
			UserID: userID,
		},
	)
	require.NoError(t, err)

	expectedURL := "https://google.ru"
	expectedTitle := "google"
	_, err = linksRepo.Update(
		ctx, database.UpdateLinkReq{
			ID:     id,
			URL:    expectedURL,
			Title:  expectedTitle,
			UserID: userID,
		},
	)
	require.NoError(t, err)

	updated, err := linksRepo.FindByID(ctx, id)
	require.NoError(t, err)

	if updated.URL != expectedURL {
		assert.Equal(t, updated.URL, expectedURL)
	}

	if updated.Title != expectedTitle {
		assert.Equal(t, updated.Title, expectedTitle)
	}
}

func TestRepository_FindByUserID(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip()
	}

	ctx := context.Background()
	userID := uuid.New().String()
	{
		id := primitive.NewObjectID()

		_, err := linksRepo.Create(
			ctx, database.CreateLinkReq{
				ID:     id,
				URL:    "https://ya.ru",
				Title:  "ya",
				UserID: userID,
			},
		)
		require.NoError(t, err)
	}
	{
		id := primitive.NewObjectID()

		_, err := linksRepo.Create(
			ctx, database.CreateLinkReq{
				ID:     id,
				URL:    "https://google.ru",
				Title:  "google",
				UserID: userID,
			},
		)
		require.NoError(t, err)
	}

	list, err := linksRepo.FindByUserID(ctx, userID)
	require.NoError(t, err)

	if len(list) < 2 {
		require.GreaterOrEqual(t, len(list), 2)
	}
}

func TestRepository_Delete(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip()
	}

	ctx := context.Background()

	id := primitive.NewObjectID()
	_, err := linksRepo.Create(
		ctx, database.CreateLinkReq{
			ID:     id,
			URL:    "https://ya.ru",
			Title:  "ya",
			UserID: uuid.New().String(),
		},
	)
	require.NoError(t, err)

	_, err = linksRepo.FindByID(ctx, id)
	require.NoError(t, err)

	err = linksRepo.Delete(ctx, id)
	require.NoError(t, err)

	_, err = linksRepo.FindByID(ctx, id)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			t.Fatal(err)
		}
	}
}
