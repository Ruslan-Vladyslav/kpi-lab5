package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/roman-mazur/architecture-practice-4-template/datastore"
	"github.com/roman-mazur/architecture-practice-4-template/httptools"
)

var port = flag.Int("port", 8082, "db HTTP port")

func main() {
	flag.Parse()

	dbDir := os.Getenv("DB_DIR")
	if dbDir == "" {
		dbDir = "./data"
	}
	db, err := datastore.Open(dbDir)
	if err != nil {
		log.Fatalf("failed to open DB: %v", err)
	}
	defer db.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/db/", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[len("/db/"):]
		switch r.Method {
		case http.MethodPost:
			var body struct {
				Value string `json:"value"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			if err := db.Put(key, body.Value); err != nil {
				http.Error(w, "put error", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			value, err := db.Get(key)
			if err == datastore.ErrKeyMissing {
				http.NotFound(w, r)
				return
			} else if err != nil {
				http.Error(w, "get error", http.StatusInternalServerError)
				return
			}
			resp := map[string]string{"key": key, "value": value}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	server := httptools.CreateServer(*port, mux)
	log.Printf("Starting DB HTTP on :%d", *port)
	server.Start()
	select {}
}
