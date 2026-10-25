package main

import (
	"bufio"
	proto "chitchat/grpc"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var lamport int64
var mu sync.Mutex // guards lamport

// local or send event
func increment() int64 {
	mu.Lock()
	defer mu.Unlock()
	lamport++
	return lamport
}

func receive(stream grpc.ServerStreamingClient[proto.ChatMessage], name string) {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Println("lost connection to server")
			log.Println("client:", name, "lost connection to server:", err)
			os.Exit(1)
		}

		// receive event
		mu.Lock()
		lamport = max(lamport, msg.Lamport) + 1
		t := lamport
		mu.Unlock()

		fmt.Printf("%s: %s (lamport %d)\n", msg.Sender, msg.Text, msg.Lamport)
		log.Printf("client: %s got message from %s: %s (lamport %d, local lamport %d)", name, msg.Sender, msg.Text, msg.Lamport, t)
	}
}

func leave(client proto.ChitChatClient, name string) {
	t := increment()
	_, err := client.Leave(context.Background(), &proto.LeaveRequest{Name: name, Lamport: t})
	if err != nil {
		log.Println("client:", name, "failed to leave:", err)
		return
	}
	log.Println("client:", name, "left at lamport", t)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("usage: go run ./client <name>")
	}
	name := os.Args[1]

	conn, err := grpc.NewClient("localhost:5050", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("client: failed to connect: %v", err)
	}
	defer conn.Close()
	client := proto.NewChitChatClient(conn)

	t := increment()
	stream, err := client.Join(context.Background(), &proto.JoinRequest{Name: name, Lamport: t})
	if err != nil {
		log.Fatalf("client: failed to join: %v", err)
	}

	// each client logs to its own file, the terminal is for the chat
	file, err := os.Create(name + ".log")
	if err != nil {
		log.Fatalf("client: failed to create log file: %v", err)
	}
	defer file.Close()
	log.SetOutput(file)
	log.Println("client:", name, "connected at lamport", t)

	go receive(stream, name)

	// leave on Ctrl+C
	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt)
		<-stop
		leave(client, name)
		os.Exit(0)
	}()

	// every line is a message, "leave" leaves the chat
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		if text == "leave" {
			break
		}
		t := increment()
		log.Println("client:", name, "publishing", text, "at lamport", t)
		_, err := client.Publish(context.Background(), &proto.PublishRequest{Name: name, Text: text, Lamport: t})
		if err != nil {
			fmt.Println("could not send message:", err)
			log.Println("client:", name, "failed to publish:", err)
		}
	}
	leave(client, name)
}
