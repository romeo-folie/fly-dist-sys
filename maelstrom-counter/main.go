/*
	Your node will need to accept two RPC-style message types: add & read.
	Your service need only be eventually consistent: given a few seconds without writes,
	it should converge on the correct counter value.
	Please note that the final read from each node should return the final & correct count.

	RPC: add
	Your node should accept add requests and increment the value of a single global counter. Your node will receive a request message body that looks like this:
	{
		"type": "add",
		"delta": 123
	}
	and it will need to return an "add_ok" acknowledgement message:
	{
  	"type": "add_ok"
	}

	RPC: read
	Your node should accept read requests and return the current value of the global counter. Remember that the counter service is only sequentially consistent.
	Your node will receive a request message body that looks like this:
	{
		"type": "read"
	}
	and it will need to return a "read_ok" message with the current value:

	{
		"type": "read_ok",
		"value": 1234
	}

*/

package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)

	node.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		newValue := int(body["delta"].(float64))
		oldValue, err := kv.ReadInt(context.Background(), node.ID())
		if err != nil {
			oldValue = 0
		}

		kv.CompareAndSwap(context.Background(), node.ID(), oldValue, oldValue+newValue, true)

		delete(body, "delta")
		body["type"] = "add_ok"

		return node.Reply(msg, body)
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		total := 0
		for _, nodeID := range node.NodeIDs() {
			count, err := kv.ReadInt(context.Background(), nodeID)
			if err != nil {
				continue
			}

			total += count
		}

		body["value"] = total
		body["type"] = "read_ok"

		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
