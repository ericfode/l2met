package piping

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"net/url"
	"os"
	"time"
)

var (
	redisPool *redis.Pool
)

func setupRedis(redisUrl string) {
	u, err := url.Parse(redisUrl)
	if err != nil {
		fmt.Printf("error=%q\n", "Missing REDIS_URL.")
		os.Exit(1)
	}
	server := u.Host
	password, set := u.User.Password()
	if !set {
		fmt.Printf("at=error error=%q\n", "password not set")
		os.Exit(1)
	}
	redisPool = &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 10 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialTimeout("tcp", server, time.Second, time.Second, time.Second)
			if err != nil {
				return nil, err
			}
			c.Do("AUTH", password)
			return c, err
		},
	}

}

func init() {
	setupRedis(os.Getenv("REDIS_URL"))
}
