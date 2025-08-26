package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	var mu sync.Mutex
	node := maelstrom.NewNode()
	linKV := maelstrom.NewLinKV(node)
	committedLogOffset := make(map[string]int)

	node.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		message := int(body["msg"].(float64))
		var offset int
		last, readErr := linKV.ReadInt(context.Background(), "offset:"+key)
		if readErr != nil {
			if maelstrom.ErrorCode(readErr) == maelstrom.KeyDoesNotExist {
				last = 0
			} else {
				return readErr
			}
		}

		for {
			next := last + 1
			casErr := linKV.CompareAndSwap(context.Background(), "offset:"+key, last, next, true)

			if casErr == nil {
				// this means that it successfully replaced the offset entry there
				offset = next
				break
			} else {
				return casErr
			}
		}

		// write the message
		if writeErr := linKV.Write(context.Background(), "log:"+key+":"+fmt.Sprint(offset), message); writeErr != nil {
			return writeErr
		}

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

		// given a starting offset
		// need to return messages starting from that offset. no limit on count
		// look through kv

		// fetch the record at that index. return it since we can return any count
		// for each one of the keys

		// to return messages from the offset in the log
		messages := make(map[string][][2]int)
		offsets, ok := body["offsets"].(map[string]any)
		if !ok {
			return fmt.Errorf("invalid or missing 'offsets' field")
		}

		for offsetKey, value := range offsets {
			startOffset := int(value.(float64))
			messages[offsetKey] = [][2]int{}

			currOffset := startOffset
			for {
				key := "log:" + offsetKey + ":" + fmt.Sprint(currOffset)
				logMsg, err := linKV.ReadInt(context.Background(), key)
				if err != nil {
					if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
						break
					}
					return err
				}
				messages[offsetKey] = append(messages[offsetKey], [2]int{currOffset, logMsg})
				currOffset += 1
			}
		}

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
		for key, value := range offsets {
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

		mu.Lock()
		for _, k := range keys {
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
