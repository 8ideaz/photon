package server

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

type Config struct {
	// Addr should be of the format localhost:8080 or 192.168.0.12:8080 or :8080
	Addr string
}

// Entry stores a value and its expiry time.
type Entry struct {
	Value      string
	ExpiryTime time.Time
}

// Database represents the in-memory data store.
type Database struct {
	data sync.Map
}

// Set a key to a value with an optional expiry time.
func (db *Database) Set(key, value string, ttl time.Duration) {
	var expiry time.Time
	if ttl > 0 {
		expiry = time.Now().Add(ttl)
	}
	db.data.Store(key, Entry{Value: value, ExpiryTime: expiry})
}

// Get a value by key, checking for expiration.
func (db *Database) Get(key string) (string, bool) {
	entryRaw, exists := db.data.Load(key)
	if !exists {
		return "", false
	}
	entry := entryRaw.(Entry)
	if !entry.ExpiryTime.IsZero() && time.Now().After(entry.ExpiryTime) {
		db.data.Delete(key)
		return "", false
	}
	return entry.Value, true
}

// Delete a key from the database.
func (db *Database) Del(key string) bool {
	_, exists := db.data.Load(key)
	if exists {
		db.data.Delete(key)
		return true
	}
	return false
}

// ProcessCommand parses and executes commands.
func (db *Database) ProcessCommand(command string) string {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return "-ERR empty command"
	}

	switch strings.ToUpper(parts[0]) {
	case "SET":
		if len(parts) < 3 {
			return "-ERR wrong number of arguments for 'SET'"
		}
		key, value := parts[1], parts[2]
		var ttl time.Duration
		if len(parts) == 5 && strings.ToUpper(parts[3]) == "PX" {
			ms, err := time.ParseDuration(parts[4] + "ms")
			if err != nil {
				return "-ERR invalid expiration time"
			}
			ttl = ms
		}
		db.Set(key, value, ttl)
		return "+OK"
	case "GET":
		if len(parts) != 2 {
			return "-ERR wrong number of arguments for 'GET'"
		}
		key := parts[1]
		value, exists := db.Get(key)
		if !exists {
			return "$-1"
		}
		return fmt.Sprintf("$%d\r\n%s", len(value), value)
	case "DEL":
		if len(parts) < 2 {
			return "-ERR wrong number of arguments for 'DEL'"
		}
		count := 0
		for _, key := range parts[1:] {
			if db.Del(key) {
				count++
			}
		}
		return fmt.Sprintf(":%d", count)
	default:
		return "-ERR unknown command"
	}
}

// Start initializes and runs the Redis-like server.
func Start(cfg *Config) error {
	ctx := context.Background()
	return startServer(ctx, cfg)
}

func startServer(ctx context.Context, cfg *Config) error {
	db := &Database{}
	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		err = fmt.Errorf("error starting server: %w", err)
		return err
	}
	defer listener.Close()

	log.Printf("Server is running on %s...", cfg.Addr)
	completed := false
	for !completed {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go handleConnection(conn, db)
		select {
		case <-ctx.Done():
			log.Printf("exiting start server")
			completed = true
		}
	}
	return nil

}

func handleConnection(conn net.Conn, db *Database) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		command, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading command: %v", err)
			return
		}
		command = strings.TrimSpace(command)
		response := db.ProcessCommand(command)
		conn.Write([]byte(response + "\r\n"))
	}
}
