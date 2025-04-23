/*
	My node will need to handle the "broadcast" workload which has 3 RPC
	message types: broadcast, read, & topology.

	My node will need to store the set of integer values that it sees from broadcast
	messages so that they can be returned later via the read message RPC

	Two methods for sending messages

	Send() - sends a fire and forget message, doesn't expect a response
	RPC() - sends a message and accepts a response handler. 
	The message will be decorated with a message ID so the handler can be invoked when a response message is received.

	RPC: broadcast
	will receive a message with a number to broadcast to all neighboring nodes
	should respond with broadcast_ok after storing value locally

	RPC: read
	this message requests that the node returns all the messages it's seen.
	should respond with read_ok and messages list

	RPC: topology
	this informs the node of it's neighbors.
	should respond with "topology_ok" on receipt
*/

package main

import (
	"log"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {

	n := maelstrom.NewNode()
	var messages []float64
	
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		
		body["type"] = "topology_ok"
		delete(body, "topology")

		return n.Reply(msg, body)
	})
	
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for _, nodeID := range n.NodeIDs() {
			if nodeID == n.ID() {
				continue
			}

			if err := n.Send(nodeID, body); err != nil {
				return err
			}
		}

		body["type"] = "broadcast_ok"
		messages = append(messages, body["message"].(float64))
		delete(body, "message")

		return n.Reply(msg, body)
	})
	
	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = messages

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}