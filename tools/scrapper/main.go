package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/pkg/scrape"
)

func main() {
	parse, err := scrape.Parse(
		context.Background(),
		"https://www.freecodecamp.org/news/how-to-build-an-online-banking-system-python-oop-tutorial/",
	)
	if err != nil {
		log.Fatal(err)
	}

	encoded, err := json.MarshalIndent(parse, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s", string(encoded))
}
