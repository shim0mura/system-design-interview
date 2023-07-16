package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var client *redis.Client
var m = sync.Map{}

type sessionKey string

const SessionKey sessionKey = "SESSION"

func initRedis() {
	client = redis.NewClient(&redis.Options{
		Addr: "localhost:6380",
	})
}
func init() {
	initRedis()
}

func sessionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sessionValue := r.Header.Get(string(SessionKey))
		if len(sessionValue) < 1 {
			sessionValue = strconv.Itoa(rand.Int() % 2)
			w.Header().Set(string(SessionKey), sessionValue)
		}
		fmt.Println("session value: " + sessionValue)
		ctx := context.WithValue(r.Context(), SessionKey, sessionValue)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func rateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		sessionValue := ctx.Value(SessionKey)
		session, ok := sessionValue.(string)
		if !ok {
			http.Error(w, "cannot get session value", http.StatusInternalServerError)
			return
		}

		// https://redis.com/glossary/rate-limiting/
		redisSessionKey := "rate:" + session + ":" + strconv.Itoa(time.Now().Minute()%10)

		if v, ok := m.Load(redisSessionKey); ok {
			fmt.Printf("sync map value detected. %#v \n", v)
			http.Error(w, "rate limit over", http.StatusTooManyRequests)
			return
		}

		// https://redis.io/commands/incr/
		// Pattern: Rate limiter 2
		result, err := client.IncrBy(ctx, redisSessionKey, 1).Result()
		if err != nil {
			http.Error(w, "something with wrong", http.StatusInternalServerError)
			return
		}

		if result > 10 {
			// rate limit超えてる場合はいちいちredisに見に行かなくていいようにinmemory cache使う
			m.Store(redisSessionKey, "10")
			http.Error(w, "rate limit over", http.StatusTooManyRequests)
			return
		} else if result == 1 {
			if result := client.Expire(ctx, redisSessionKey, time.Second*60); !result.Val() {
				http.Error(w, "something with wrong", http.StatusInternalServerError)
				return
			}
		}

		fmt.Printf("session %#v redisSessionKey: %#v value: %#v \n", session, redisSessionKey, result)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
func main() {
	helloHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, "hello")
	})
	http.Handle("/hello", sessionMiddleware(rateLimitMiddleware(helloHandler)))
	log.Fatal(http.ListenAndServe(":8080", nil))
}
