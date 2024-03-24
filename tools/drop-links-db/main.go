package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/sethvargo/go-envconfig"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/env/config"
)

var linksDB string

func init() {
	flag.StringVar(&linksDB, "l", "links", "-l links")
	flag.Parse()
}

func main() {
	var cfg config.Config
	ctx := context.Background()
	if err := envconfig.Process(ctx, &cfg); err != nil { //nolint:typecheck
		log.Fatal(err)
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
		log.Fatal(err)
	}

	if linksDB != "" {
		if err := linksDBConn.Database(linksDB).Drop(ctx); err != nil {
			return
		}
	}
}
