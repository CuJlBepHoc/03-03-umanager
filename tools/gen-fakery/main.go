package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/bxcodec/faker/v4"

	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/database"
)

var (
	linksNum int
)

func init() {
	flag.IntVar(&linksNum, "ln", 10, "-ln 10")
	flag.Parse()
}

func main() {
	linksCollection := make([]database.CreateLinkReq, 0, linksNum)
	for i := 0; i < linksNum; i++ {
		url := faker.URL()

		linksCollection = append(
			linksCollection, database.CreateLinkReq{
				ID:     primitive.NewObjectID(),
				Title:  url,
				URL:    url,
				UserID: uuid.New().String(),
			},
		)
	}

	marshal, err := json.MarshalIndent(linksCollection, "", "  ")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%s", string(marshal))
}
