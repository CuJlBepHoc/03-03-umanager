package env

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sethvargo/go-envconfig"
	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/apigw/routes"
	v1 "gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/apigw/v1"
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/database/links"
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/database/users"
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/env/config"
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/link/linkgrpc"
	link_updater "gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/link/stories/link-updater"
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/user/usergrpc"
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/pkg/pb"
)

type Env struct {
	Config          config.Config
	ApiGWHTTPServer *http.Server
	LinksGRPCServer *grpc.Server
	UsersGRPCServer *grpc.Server
	LinkUpdater     *link_updater.Story
}

func Setup(ctx context.Context) (*Env, error) {
	var cfg config.Config
	env := &Env{}

	if err := envconfig.Process(ctx, &cfg); err != nil { //nolint:typecheck
		return nil, fmt.Errorf("env processing: %w", err)
	}

	linksDBConn, err := mongo.Connect(
		ctx, &options.ClientOptions{
			ConnectTimeout: &cfg.LinksService.Mongo.ConnectTimeout,
			Hosts:          []string{fmt.Sprintf("%s:%d", cfg.LinksService.Mongo.Host, cfg.LinksService.Mongo.Port)},
			MaxPoolSize:    &cfg.LinksService.Mongo.MaxPoolSize,
			MinPoolSize:    &cfg.LinksService.Mongo.MinPoolSize,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("mongo.Connect: %w", err)
	}

	usersDBConn, err := pgxpool.Connect(ctx, cfg.UsersService.Postgres.ConnectionURL())
	if err != nil {
		return nil, fmt.Errorf("pgxpool Connect: %w", err)
	}

	amqpConn, err := amqp.Dial(cfg.LinksService.AMQP.String())
	if err != nil {
		return nil, fmt.Errorf("amqp Dial: %w", err)
	}

	defer amqpConn.Close()

	amqpChannel, err := amqpConn.Channel()
	if err != nil {
		return nil, fmt.Errorf("amqp Channel: %w", err)
	}

	// задекларируйте очередь
	// amqpChannel.QueueDeclare()

	usersRepository := users.New(usersDBConn, 5*time.Second) // вынести в конфиг duration
	linksRepository := links.New(
		linksDBConn.Database(cfg.LinksService.Mongo.Name),
		5*time.Second, // вынести в конфиг duration
	)

	{
		handler := linkgrpc.New(linksRepository, cfg.LinksService.GRPCServer.Timeout, amqpChannel)

		s := grpc.NewServer()
		reflection.Register(s) // этот код нужен для дебаггинга
		pb.RegisterLinkServiceServer(s, handler)

		// grpc server start function
		env.LinksGRPCServer = s
	}

	{
		handler := usergrpc.New(usersRepository, cfg.LinksService.GRPCServer.Timeout)

		s := grpc.NewServer()
		reflection.Register(s) // этот код нужен для дебаггинга
		pb.RegisterUserServiceServer(s, handler)

		// grpc server start function
		env.UsersGRPCServer = s
	}

	// Инициализируем клиенты GRPC

	// Клиент для осуществления запросов в users service
	usersClientConn, err := grpc.DialContext(
		ctx, cfg.ApiGWService.UsersClientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc DialContext: %w", err)
	}

	usersClient := pb.NewUserServiceClient(usersClientConn)

	// Клиент для осуществления запросов в links service
	linksClientConn, err := grpc.DialContext(
		ctx, cfg.ApiGWService.LinksClientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc DialContext: %w", err)
	}

	linksClient := pb.NewLinkServiceClient(linksClientConn)

	// API GW handler
	// В роуйтере пакета v1 нужно использовать клиенты и запрашивать данные с сервисов links и users
	handler := v1.New(usersClient, linksClient)
	router := routes.Router(handler)

	apiGWServer := &http.Server{
		Addr:              cfg.ApiGWService.Addr,
		Handler:           router,
		ReadTimeout:       cfg.ApiGWService.ReadTimeout,
		ReadHeaderTimeout: cfg.ApiGWService.ReadTimeout,
		WriteTimeout:      cfg.ApiGWService.WriteTimeout,
		IdleTimeout:       cfg.ApiGWService.ReadTimeout,
	}

	linkUpdaterStory := link_updater.New(linksRepository, amqpChannel)

	env.ApiGWHTTPServer = apiGWServer
	env.Config = cfg
	env.LinkUpdater = linkUpdaterStory

	return env, nil
}
