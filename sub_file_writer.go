package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	zmq "github.com/pebbe/zmq4"
)

const (
	publisherIP    = "10.3.192.105" // or "127.0.0.1" for local testing
	topicFilter    = "tt_data"
	outputDir      = "data"
	maxFileSize    = 500 * 1024 * 1024 // 100MB per file
	fileRotateTime = 1 * time.Hour
)

var (
	messageCount  uint64
	byteCount     uint64
	currentFile   *os.File
	currentSize   int64
	fileCounter   int
	startTime     = time.Now()
	lastPrintTime = time.Now()
	lastRotate    = time.Now()
)

func main() {
	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Setup ZMQ
	context, err := zmq.NewContext()
	if err != nil {
		log.Fatalf("Failed to create ZMQ context: %v", err)
	}
	defer context.Term()

	subscriber, err := context.NewSocket(zmq.SUB)
	if err != nil {
		log.Fatalf("Failed to create subscriber socket: %v", err)
	}
	defer subscriber.Close()

	// Connect and subscribe
	connStr := fmt.Sprintf("tcp://%s:5555", publisherIP)
	fmt.Printf("Connecting to %s...\n", connStr)
	if err := subscriber.Connect(connStr); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	if err := subscriber.SetSubscribe(topicFilter); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	fmt.Printf("Subscribed to topic: '%s'\n", topicFilter)

	// Open first file
	if err := rotateFile(); err != nil {
		log.Fatalf("Failed to open initial file: %v", err)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nShutting down subscriber...")
		closeCurrentFile()
		subscriber.Close()
		context.Term()
		os.Exit(0)
	}()

	// Main receive loop
	for {
		parts, err := subscriber.RecvBytes(0)
		if err != nil {
			log.Printf("Error receiving message: %v", err)
			continue
		}

		if len(parts) > 1 {
			msg := parts[1]//exclude topic name
			processMessage(msg)
		}
	}
}

func processMessage(msg []byte) {
	// Update counters
	atomic.AddUint64(&messageCount, 1)
	atomic.AddUint64(&byteCount, uint64(len(msg)))

	// Write to file
	if _, err := currentFile.Write(msg); err != nil {
		log.Printf("Error writing to file: %v", err)
		if err := rotateFile(); err != nil {
			log.Printf("Failed to rotate file: %v", err)
		}
		return
	}

	// Update file size and check rotation
	currentSize += int64(len(msg))
	now := time.Now()

	// Rotate if needed
	if currentSize >= maxFileSize || now.Sub(lastRotate) >= fileRotateTime {
		if err := rotateFile(); err != nil {
			log.Printf("Failed to rotate file: %v", err)
		} else {
			lastRotate = now
		}
	}

	// Print stats every second
	if now.Sub(lastPrintTime) >= time.Second {
		printStats()
		lastPrintTime = now
	}
}

func rotateFile() error {
	closeCurrentFile()

	// Generate new filename
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join(outputDir, fmt.Sprintf("data_%s_%d.bin", timestamp, fileCounter))
	fileCounter++

	// Open new file
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", filename, err)
	}

	currentFile = f
	currentSize = 0
	fmt.Printf("Opened new output file: %s\n", filename)
	return nil
}

func closeCurrentFile() {
	if currentFile != nil {
		if err := currentFile.Close(); err != nil {
			log.Printf("Error closing file: %v", err)
		}
		fmt.Printf("Closed file, size: %.2f MB\n", float64(currentSize)/(1024*1024))
		currentFile = nil
		currentSize = 0
	}
}

func printStats() {
	//elapsed := time.Since(startTime).Seconds()
	mCount := atomic.SwapUint64(&messageCount, 0)
	bCount := atomic.SwapUint64(&byteCount, 0)

	//msgRate := float64(mCount) / elapsed
	//dataRate := float64(bCount) / elapsed
	//dataRateKbps := dataRate * 8 / 1024

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	fmt.Printf("[%s] Messages: %d | Data: %d Bps | File: %.2f MB\n",
		timestamp, mCount, bCount, float64(currentSize)/(1024*1024))
}
