package users

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-playground/assert/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sethvargo/go-envconfig"
	"github.com/stretchr/testify/require"

	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/database"
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/env/config"
)

var (
	usersRepo *Repository
	conn      *pgxpool.Pool
	once      sync.Once
)

var rnd *rand.Rand
var mu sync.Mutex

func init() {
	rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
}

func generateUser() (database.CreateUserReq, error) {
	buf := make([]byte, 16)
	mu.Lock()
	if _, err := rnd.Read(buf); err != nil {
		mu.Unlock()
		return database.CreateUserReq{}, fmt.Errorf("rand Read: %w", err)
	}
	mu.Unlock()
	return database.CreateUserReq{
		ID:       uuid.New(),
		Username: hex.EncodeToString(buf),
		Password: "password",
	}, nil
}

func TestMain(m *testing.M) {
	ctx := context.Background()

	once.Do(
		func() {
			var cfg config.Config
			if err := envconfig.Process(ctx, &cfg); err != nil { //nolint:typecheck
				log.Fatalf("env processing: %v", err)
			}

			usersDBConn, err := pgxpool.Connect(ctx, cfg.UsersService.Postgres.ConnectionURL())
			if err != nil {
				log.Fatal(err)
			}

			conn = usersDBConn

			usersRepo = New(usersDBConn, 5*time.Second)
		},
	)
	exitCode := m.Run()
	conn.Close()
	os.Exit(exitCode)
}

func TestRepository_Create(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip()
	}

	ctx := context.Background()

	u, err := generateUser()
	require.NoError(t, err)

	{
		_, err := usersRepo.Create(
			ctx, database.CreateUserReq{
				ID:       u.ID,
				Username: u.Username,
				Password: u.Password,
			},
		)
		require.NoError(t, err)

		created, err := usersRepo.FindByID(ctx, u.ID)
		require.NoError(t, err)

		assert.Equal(t, created.ID, u.ID)
		assert.Equal(t, created.Username, u.Username)
		assert.Equal(t, created.Password, u.Password)
	}
	{
		_, err := usersRepo.Create(
			ctx, database.CreateUserReq{
				ID:       u.ID,
				Username: u.Username,
				Password: u.Password,
			},
		)
		require.NoError(t, err)
	}
}

func TestRepository_FindAll(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip()
	}

	ctx := context.Background()

	u, err := generateUser()
	require.NoError(t, err)

	_, err = usersRepo.Create(
		ctx, database.CreateUserReq{
			ID:       u.ID,
			Username: u.Username,
			Password: u.Password,
		},
	)
	require.NoError(t, err)

	all, err := usersRepo.FindAll(ctx)
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(all), 1)
}

func TestRepository_DeleteByUserID(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip()
	}

	ctx := context.Background()

	u, err := generateUser()
	require.NoError(t, err)

	_, err = usersRepo.Create(
		ctx, database.CreateUserReq{
			ID:       u.ID,
			Username: u.Username,
			Password: u.Password,
		},
	)
	require.NoError(t, err)

	{
		_, err := usersRepo.FindByID(ctx, u.ID)
		require.NoError(t, err)
	}

	if err := usersRepo.DeleteByUserID(ctx, u.ID); err != nil {
		t.Fatal(err)
	}

	{
		_, err := usersRepo.FindByID(ctx, u.ID)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				t.Fatal(err)
			}
		}
	}
}
