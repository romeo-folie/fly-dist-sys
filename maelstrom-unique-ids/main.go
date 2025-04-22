package main

import (
	"encoding/json"
	"time"
	"math/rand"
	"log"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func generateID() int64 {
	return rand.Int63() + time.Now().UnixNano();
}

func main() {
	n := maelstrom.NewNode()

	// generate a globally unique ID
	// should set type to generate_ok in the response
	// should return an extra field named "id" with the generated ID

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = generateID()

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

