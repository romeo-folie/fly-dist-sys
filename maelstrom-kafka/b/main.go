package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()
	linKV := maelstrom.NewLinKV(node)
	seqKv := maelstrom.NewSeqKV(node)

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
				offset = next
				break
			} else {
				if maelstrom.ErrorCode(casErr) != maelstrom.PreconditionFailed {
					return casErr
				}
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

		for key, value := range offsets {
			val := int(value.(float64))
			writeErr := seqKv.Write(context.Background(), key, val)
			if writeErr != nil {
				return writeErr
			}
		}

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

		for _, k := range keys {
			key := k.(string)
			offset, err := seqKv.ReadInt(context.Background(), key)
			if err != nil {
				continue
			}

			offsets[key] = offset
		}

		delete(body, "keys")
		body["type"] = "list_committed_offsets_ok"
		body["offsets"] = offsets

		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

}
