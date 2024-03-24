package config

import (
	"fmt"
	"net/url"
	"strconv"
	"time"
)

type Config struct {
	UsersService UsersService `env:",prefix=USERS_"`
	LinksService LinksService `env:",prefix=LINKS_"`
	ApiGWService ApiGWService `env:",prefix=APIGW_"`
}

type LinksService struct {
	Mongo      MongoConfig     `env:",prefix=DB_"`
	GRPCServer LinksGRPCConfig `env:",prefix=GRPC_"`
}

type LinksGRPCConfig struct {
	Addr    string        `env:"ADDR,default=:51000"`
	Timeout time.Duration `env:"TIMEOUT,default=10s"`
}

type MongoConfig struct {
	Name           string        `env:"NAME,default=links"`
	Host           string        `env:"HOST,default=127.0.0.1"`
	Port           int           `env:"PORT,default=27018"`
	User           string        `env:"USER,default=mongo"`
	Password       string        `env:"USER,default=mongo"`
	MinPoolSize    uint64        `env:"MIN_POOL_SIZE,default=5"`
	MaxPoolSize    uint64        `env:"MAX_POOL_SIZE,default=50"`
	ConnectTimeout time.Duration `env:"CONNECT_TIMEOUT,default=5s"`
}

func (m MongoConfig) ConnectionString() string {
	return fmt.Sprintf("mongodb://%s:%d", m.Host, m.Port)
}

type UsersService struct {
	Postgres   PostgresConfig  `env:",prefix=DB_"`
	GRPCServer UsersGRPCConfig `env:",prefix=GRPC_"`
}

type UsersGRPCConfig struct {
	Addr    string        `env:"ADDR,default=:52000"`
	Timeout time.Duration `env:"TIMEOUT,default=10s"`
}

type PostgresConfig struct {
	Name         string        `env:"NAME,default=users" json:",omitempty"`
	User         string        `env:"USER,default=postgres" json:",omitempty"`
	Host         string        `env:"HOST,default=localhost" json:",omitempty"`
	Port         int           `env:"PORT,default=5434" json:",omitempty"`
	SSLMode      string        `env:"SSLMODE,default=disable" json:",omitempty"`
	ConnTimeout  int           `env:"CONN_TIMEOUT,default=5" json:",omitempty"`
	Password     string        `env:"PASSWORD,default=postgres" json:"-"`
	PoolMinConns int           `env:"POOL_MIN_CONNS,default=10" json:",omitempty"`
	PoolMaxConns int           `env:"POOL_MAX_CONNS,default=50" json:",omitempty"`
	DBTimeout    time.Duration `env:"TIMEOUT,default=5s"`
}

func (c PostgresConfig) ConnectionURL() string {
	host := c.Host
	if v := c.Port; v != 0 {
		host = host + ":" + strconv.Itoa(c.Port)
	}

	u := &url.URL{
		Scheme: "postgres",
		Host:   host,
		Path:   c.Name,
	}

	if c.User != "" || c.Password != "" {
		u.User = url.UserPassword(c.User, c.Password)
	}

	q := u.Query()
	if v := c.ConnTimeout; v > 0 {
		q.Add("connect_timeout", strconv.Itoa(v))
	}
	if v := c.SSLMode; v != "" {
		q.Add("sslmode", v)
	}

	u.RawQuery = q.Encode()

	return u.String()
}

type ApiGWService struct {
	Addr            string        `env:"ADDR,default=:8080"`
	ReadTimeout     time.Duration `env:"READ_TIMEOUT,default=30s"`
	WriteTimeout    time.Duration `env:"WRITE_TIMEOUT,default=30s"`
	UsersClientAddr string        `env:"USERS_CLIENT_ADDR,default=:52000"`
	LinksClientAddr string        `env:"USERS_CLIENT_ADDR,default=:51000"`
}
