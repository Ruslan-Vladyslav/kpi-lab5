package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	team := os.Getenv("TEAM_NAME")
	if team == "" {
		log.Fatal("TEAM_NAME must be set")
	}
	today := time.Now().Format("2006-01-02")
	payload, _ := json.Marshal(map[string]string{"value": today})
	resp, err := http.DefaultClient.Post(
		fmt.Sprintf("http://db:8082/db/%s", team),
		"application/json",
		bytes.NewReader(payload),
	)
	if err != nil {
		log.Fatalf("failed to seed DB: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("failed to seed DB: status=%d", resp.StatusCode)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.NotFound(rw, r)
			return
		}

		dbURL := fmt.Sprintf("http://db:8082/db/%s", key)
		resp, err := http.DefaultClient.Get(dbURL)
		if err != nil {
			http.Error(rw, "db error", http.StatusServiceUnavailable)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			http.NotFound(rw, r)
			return
		}
		var entry struct{ Key, Value string }
		if err := json.NewDecoder(resp.Body).Decode(&entry); err != nil {
			http.Error(rw, "bad db reply", http.StatusInternalServerError)
			return
		}

		rw.Header().Set("Content-Type", "application/json")
		json.NewEncoder(rw).Encode([]string{entry.Value})
	})

	log.Printf("Starting server on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
