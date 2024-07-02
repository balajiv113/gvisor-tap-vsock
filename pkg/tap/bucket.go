package tap

import (
	"github.com/sirupsen/logrus"
	"time"
)

// TokenBucket struct to implement rate limiting and message queue
type TokenBucket struct {
	capacity     int
	tokens       int
	rate         time.Duration
	ticker       *time.Ticker
	addToken     chan struct{}
	messageQueue chan []byte
}

// NewTokenBucket creates a new TokenBucket
func NewTokenBucket(capacity int, rate time.Duration) *TokenBucket {
	tb := &TokenBucket{
		capacity:     capacity,
		tokens:       0,
		ticker:       time.NewTicker(rate),
		addToken:     make(chan struct{}, capacity),
		messageQueue: make(chan []byte, 100), // Adjust buffer size as needed
	}
	go tb.refill()

	return tb
}

// refill method to add tokens at regular intervals
func (tb *TokenBucket) refill() {
	for range tb.ticker.C {
		if tb.tokens < tb.capacity {
			tb.tokens++
			tb.addToken <- struct{}{}
		}
	}
}

// WaitAndSend method to wait for a token and send a message
func (tb *TokenBucket) WaitAndSend(fn func(buf []byte) error) {
	for msg := range tb.messageQueue {
		<-tb.addToken
		tb.tokens--
		err := fn(msg)
		if err != nil {
			logrus.Println("Error writing to connection:", err)
		}
	}
}
