package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"reflect"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
)

type Result struct {
	Resolver string
	Records  []string
	Error    error
}

func getCachedReverseDNSRecords(conn redis.Conn, ip string) ([]string, error) {
	records, err := getReverseDNSRecords(conn, ip)
	if err == nil {
		log.Printf("Returning cached %v\n", records)
		return records, err
	}
	log.Printf("Returning uncached %v\n", records)
	records, err = lookup(ip)
	if err == nil {
		setReverseDNSRecords(ip, records)
		log.Printf("Setting in cache %v\n", records)
	}
	return records, err
}

func dNSDialer(resolver string) func(ctx context.Context, network, address string) (net.Conn, error) {
	f := func(ctx context.Context, network, address string) (net.Conn, error) {
		d := net.Dialer{}
		return d.DialContext(ctx, "udp", resolver+":53")
	}
	return f
}

func lookupReverseDNSRecordsWithDNSDialer(resolver string, c chan Result, ip string) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	r := net.Resolver{
		PreferGo: true,
		Dial:     dNSDialer(resolver),
	}
	records, err := r.LookupAddr(ctx, ip)
	c <- Result{
		resolver,
		records,
		err,
	}
}

func lookup(ip string) ([]string, error) {
	var records []string
	c := make(chan Result)
	resolvers := []string{
		"8.8.8.8",
		"208.67.220.220",
		"9.9.9.9",
		"185.228.168.168",
		"216.146.35.35",
		"156.154.71.1",
		"4.2.2.1",
	}
	for _, resolver := range resolvers {
		go lookupReverseDNSRecordsWithDNSDialer(resolver, c, ip)
	}
	for i := 0; i < len(resolvers); i++ {
		select {
		case result := <-c:
			log.Println("Select -> ", result.Resolver, result.Error, result.Records)
			if result.Error == nil {
				return result.Records, nil
			}
		case <-time.After(time.Millisecond * time.Duration(*timeout)):
			log.Println("All timed out")
			return nil, errors.New("Timeout")
		}
	}
	log.Println("All resolvers errored")
	return records, nil
}

func getReverseDNSRecords(conn redis.Conn, ip string) ([]string, error) {
	var records []string
	value, err := redis.Bytes(conn.Do("GET", ip))
	if err = json.Unmarshal(value, &records); err != nil {
		return records, err
	}
	return records, err
}

var jobs = make(chan map[string]string, *batchSize)

func setReverseDNSRecords(ip string, records []string) error {
	recordJSON, _ := json.Marshal(records)
	update := map[string]string{
		ip: string(recordJSON),
	}
	jobs <- update
	return nil
}

func ifExists(slice []map[string]string, i map[string]string) bool {
	for _, ele := range slice {
		if reflect.DeepEqual(ele, i) {
			return true
		}
	}
	return false
}

func worker(conn redis.Conn, jobs <-chan map[string]string) {
	var items = make([]map[string]string, 0, *batchSize)
	for {
		select {
		case j := <-jobs:
			if !ifExists(items, j) {
				items = append(items, j)
				log.Println("Got one more, total items:", len(items))
				if len(items) >= *batchSize {
					log.Println("Commiting on batch size", len(items), "items...")
					saveBatch(conn, items)
					items = items[:0]
				}
			}
		case <-time.After(time.Second * time.Duration(*batchDuration)):
			if len(items) > 0 {
				log.Println("Commiting on time delay", len(items), "items...")
				saveBatch(conn, items)
				items = items[:0]
			}
		}
	}
}

func saveBatch(conn redis.Conn, items []map[string]string) {
	for _, j := range items {
		for k, v := range j {
			conn.Do("SET", k, v)
		}
	}
}

var (
	redisAddress   = flag.String("redis-address", "localhost:6379", "Address to the Redis server")
	maxConnections = flag.Int("max-connections", 10, "Max connections to Redis")
	timeout        = flag.Int("timeout", 500, "DNS resolution timeout in milliseconds")
	batchSize      = flag.Int("batch-size", 50, "Save after every N unique records")
	batchDuration  = flag.Int("batch-duration", 5, "Save after every M seconds")
)

func main() {
	flag.Parse()
	redisPool := redis.NewPool(func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", *redisAddress)

		if err != nil {
			return nil, err
		}

		return c, err
	}, *maxConnections)

	conn := redisPool.Get()
	defer conn.Close()
	go worker(conn, jobs)
	r := mux.NewRouter()
	r.HandleFunc("/{ip}", index(redisPool))
	log.Println("Listening at", *redisAddress)
	srv := &http.Server{
		Handler:      r,
		Addr:         "0.0.0.0:8080",
		WriteTimeout: time.Second * 2,
		ReadTimeout:  time.Second * 2,
		IdleTimeout:  time.Second * 2,
	}
	log.Fatal(srv.ListenAndServe())
}

func index(pool *redis.Pool) http.HandlerFunc {
	fn := func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		ip := vars["ip"]
		conn := pool.Get()
		defer conn.Close()
		records, err := getCachedReverseDNSRecords(conn, ip)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			log.Println("Error", err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Internal server error"))
			return
		}
		json.NewEncoder(w).Encode(records)
	}
	return http.HandlerFunc(fn)
}
