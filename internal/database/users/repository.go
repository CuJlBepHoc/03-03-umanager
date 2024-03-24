package users

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/database"
)

func New(userDB *pgxpool.Pool, timeout time.Duration) *Repository {
	return &Repository{db: userDB, timeout: timeout}
}

type Repository struct {
	db      *pgxpool.Pool
	timeout time.Duration
}

// Create этот метод создает пользователя и обновляет его, если такой id уже существует, используйте его для обновления
func (r *Repository) Create(ctx context.Context, req database.CreateUserReq) (database.User, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	now := time.Now()
	u := database.User{
		ID:        req.ID,
		Username:  req.Username,
		Password:  req.Password,
		CreatedAt: now,
		UpdatedAt: now,
	}

	query := `
		INSERT INTO users (id, username, password, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO UPDATE
		SET username = $2, password = $3, updated_at = $5
	`
	if _, err := r.db.Exec(ctx, query, u.ID, u.Username, u.Password, now, now); err != nil {
		var writerErr *pgconn.PgError
		if errors.As(err, &writerErr) && writerErr.Code == "23505" {
			return u, database.ErrConflict
		}
		return u, fmt.Errorf("postgres Exec: %w", err)
	}

	return u, nil
}

func (r *Repository) DeleteByUserID(ctx context.Context, userID uuid.UUID) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	query := `DELETE FROM users WHERE id=$1`
	if _, err := r.db.Exec(ctx, query, userID); err != nil {
		return fmt.Errorf("postgres Exec: %w", err)
	}
	return nil
}

func (r *Repository) FindByID(ctx context.Context, userID uuid.UUID) (database.User, error) {
	var u database.User

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	if err := r.db.QueryRow(ctx, `SELECT * FROM users WHERE id=$1`, userID).Scan(
		&u.ID, &u.Username,
		&u.Password, &u.CreatedAt, &u.UpdatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return u, database.ErrNotFound
		}

		return u, fmt.Errorf("postgres QueryRow Decode: %w", err)
	}

	return u, nil
}

func (r *Repository) FindAll(ctx context.Context) ([]database.User, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	var users []database.User

	query := `SELECT id, username, password, created_at, updated_at FROM users`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("postgres Query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var user database.User
		err := rows.Scan(&user.ID, &user.Username, &user.Password, &user.CreatedAt, &user.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		users = append(users, user)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during rows iteration: %w", err)
	}

	return users, nil
}

func (r *Repository) FindByUsername(ctx context.Context, username string) (database.User, error) {
	var u database.User

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	if err := r.db.QueryRow(ctx, `SELECT * FROM users WHERE username=$1`, username).Scan(
		&u.ID, &u.Username,
		&u.Password, &u.CreatedAt, &u.UpdatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return u, database.ErrNotFound
		}
		return u, fmt.Errorf("postgres QueryRow Decode: %w", err)
	}

	return u, nil
}
