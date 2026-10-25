package main

import (
	proto "chitchat/grpc"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"unicode/utf8"

	"google.golang.org/grpc"
)

type chitChatServer struct {
	proto.UnimplementedChitChatServer
	clients map[string]chan *proto.ChatMessage
	mu      sync.Mutex
	lamport int64
}

// the caller must hold s.mu
func (s *chitChatServer) broadcast(sender string, text string) {
	msg := &proto.ChatMessage{Sender: sender, Text: text, Lamport: s.lamport}
	for _, ch := range s.clients {
		ch <- msg
	}
	log.Println("server: broadcast", text, "at lamport", s.lamport)
}

func (s *chitChatServer) Join(in *proto.JoinRequest, stream grpc.ServerStreamingServer[proto.ChatMessage]) error {
	s.mu.Lock()
	// receive event
	s.lamport = max(s.lamport, in.Lamport) + 1
	ch := make(chan *proto.ChatMessage, 10)
	s.clients[in.Name] = ch
	log.Println("server:", in.Name, "connected at lamport", s.lamport)

	// send event
	s.lamport++
	s.broadcast("Server", fmt.Sprintf("Participant %s joined Chit Chat at logical time %d", in.Name, s.lamport))
	s.mu.Unlock()

	// runs until Leave closes the channel
	for msg := range ch {
		if err := stream.Send(msg); err != nil {
			log.Println("server: failed to send to", in.Name, err)
		}
	}
	return nil
}

func (s *chitChatServer) Publish(ctx context.Context, in *proto.PublishRequest) (*proto.Ack, error) {
	// a valid message is UTF-8 and at most 128 characters
	if !utf8.ValidString(in.Text) || utf8.RuneCountInString(in.Text) > 128 {
		return nil, fmt.Errorf("invalid message")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// receive event
	s.lamport = max(s.lamport, in.Lamport) + 1
	log.Println("server: got message from", in.Name, "at lamport", s.lamport)

	// send event
	s.lamport++
	s.broadcast(in.Name, in.Text)
	return &proto.Ack{}, nil
}

func (s *chitChatServer) Leave(ctx context.Context, in *proto.LeaveRequest) (*proto.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, ok := s.clients[in.Name]
	if !ok {
		return nil, fmt.Errorf("unknown participant %s", in.Name)
	}

	// receive event
	s.lamport = max(s.lamport, in.Lamport) + 1
	delete(s.clients, in.Name)
	close(ch) // ends the Join loop for this client
	log.Println("server:", in.Name, "disconnected at lamport", s.lamport)

	// send event
	s.lamport++
	s.broadcast("Server", fmt.Sprintf("Participant %s left Chit Chat at logical time %d", in.Name, s.lamport))
	return &proto.Ack{}, nil
}

func main() {
	listener, err := net.Listen("tcp", ":5050")
	if err != nil {
		log.Fatalf("server: failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	server := &chitChatServer{clients: make(map[string]chan *proto.ChatMessage)}
	proto.RegisterChitChatServer(grpcServer, server)

	// stop the server on Ctrl+C
	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt)
		<-stop
		log.Println("server: shutting down")
		grpcServer.Stop()
	}()

	log.Println("server: listening on :5050")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("server: failed to serve: %v", err)
	}
	log.Println("server: stopped")
}
