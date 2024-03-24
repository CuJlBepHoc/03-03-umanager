package main

import (
	"fmt"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func main() {
	fmt.Println(uuid.New())
	fmt.Println(primitive.NewObjectID())
}
