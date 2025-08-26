/*
Implement a replicated log service similar to Kafka.
Replicated logs are often used as a message bus or an event stream.

Your nodes will need to store an append-only log in order to handle the "kafka" workload.
Each log is identified by a string key (e.g. "k1") and these logs contain a series of messages which are identified by an integer offset.
These offsets can be sparse in that not every offset must contain a message.

Maelstrom will check to make sure several anomalies do not occur:

Lost writes: for example, a client sees offset 10 but not offset 5.
Monotonic increasing offsets: an offset for a log should always be increasing

	log := {
		"K1": [[1000, 1]],
		"K2": [[]]
	}

	RPC: send
	This message requests that a "msg" value be appended to a log identified by "key".
	Your node will receive a request message body that looks like this:

		{
			"type": "send",
			"key": "k1",
			"msg": 123
		}

	In response, it should send an acknowledge with a send_ok message that contains the unique offset for the message in the log:

		{
			"type": "send_ok",
			"offset": 1000
		}

	RPC: poll
	This message requests that a node return messages from a set of logs starting from the given offset in each log.
	Your node will receive a request message body that looks like this:

		{
			"type": "poll",
			"offsets": {
				"k1": 1000,
				"k2": 2000
			}
		}

	In response, it should return a poll_ok message with messages starting from the given offset for each log.
	Your server can choose to return as many messages for each log as it chooses:

	{
		"type": "poll_ok",
		"msgs": {
			"k1": [[1000, 9], [1001, 5], [1002, 15]],
			"k2": [[2000, 7], [2001, 2]]
		}
	}

	RPC: commit_offsets
	This message informs the node that messages have been successfully processed up to and including the given offset. Your node will receive a request message body that looks like this:

		{
			"type": "commit_offsets",
			"offsets": {
				"k1": 1000,
				"k2": 2000
			}
		}

	In this example, the messages have been processed up to and including offset 1000 for log k1 and all messages up to and including offset 2000 for k2.

	In response, your node should return a commit_offsets_ok message body to acknowledge the request:

		{
			"type": "commit_offsets_ok"
		}


	RPC: list_committed_offsets
	This message returns a map of committed offsets for a given set of logs. Clients use this to figure out where to start consuming from in a given log.

	Your node will receive a request message body that looks like this:

	{
		"type": "list_committed_offsets",
		"keys": ["k1", "k2"]
	}
	In response, your node should return a list_committed_offsets_ok message body containing a map of offsets for each requested key. Keys that do not exist on the node can be omitted.

	{
		"type": "list_committed_offsets_ok",
		"offsets": {
			"k1": 1000,
			"k2": 2000
		}
	}
*/
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()
	logs := make(map[string][][2]int)
	committedLogOffset := make(map[string]int)
	var mu sync.Mutex

	node.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		message := int(body["msg"].(float64))

		mu.Lock()
		offset := len(logs[key])
		logs[key] = append(logs[key], [2]int{offset, message})
		mu.Unlock()

		delete(body, "key")
		delete(body, "msg")

		body["type"] = "send_ok"
		body["offset"] = offset

		return node.Reply(msg, body)
	})

	node.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages := make(map[string][][2]int)
		offsets, ok := body["offsets"].(map[string]any)
		if !ok {
			return fmt.Errorf("invalid or missing 'offsets' field")
		}

		mu.Lock()
		for key, value := range(offsets) {
			val := int(value.(float64))
			messages[key] = [][2]int{}

			if logRows, ok := logs[key]; ok && logRows != nil {
				for i, logRow := range(logRows) {
					if logRow[0] >= val {
						messages[key] = logRows[i:]
						break
					}
				}
			}
		}
		mu.Unlock()

		delete(body, "offsets")
		body["type"] = "poll_ok"
		body["msgs"] = messages

		return node.Reply(msg, body)
	})

	node.Handle("commit_offsets", func(msg maelstrom.Message) error { 
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets, ok := body["offsets"].(map[string]any)
		if !ok {
			return fmt.Errorf("invalid or missing 'offsets' field")
		}

		mu.Lock()
		for key, value := range(offsets) {
			val := int(value.(float64))
			committedLogOffset[key] = val
		}
		mu.Unlock()

		delete(body, "offsets")
		body["type"] = "commit_offsets_ok"

		return node.Reply(msg, body)
	})

	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := make(map[string]int)
		keys, ok := body["keys"].([]any)
		if !ok {
			return fmt.Errorf("invalid or missing 'keys' field")
		}

		// turns out go maps aren't thread safe for concurrent reads
		mu.Lock()
		for _, k := range(keys) {
			key := k.(string)
			offsets[key] = committedLogOffset[key]
		}
		mu.Unlock()

		delete(body, "keys")
		body["type"] = "list_committed_offsets_ok"
		body["offsets"] = offsets

		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
