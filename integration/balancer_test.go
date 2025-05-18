package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}
	seedDB(t)
	serverHits := make(map[string]int)
	requestCount := 10

	for i := 0; i < requestCount; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=%d", baseAddress, i))
		if err != nil {
			t.Fatalf("Request %d failed: %v", i, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}
		server := resp.Header.Get("lb-from")
		if server == "" {
			t.Errorf("No lb-from header in response %d", i)
		}
		serverHits[server]++
		t.Logf("Response %d from server: %s", i, server)
		resp.Body.Close()
	}

	if len(serverHits) < 2 {
		t.Errorf("Expected responses from 2 or more servers, got from: %v", serverHits)
	} else {
		t.Logf("Requests distributed across servers: %v", serverHits)
	}
}

func BenchmarkBalancer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=%d", baseAddress, i))
		if err != nil {
			b.Errorf("Request failed: %v", err)
		} else {
			resp.Body.Close()
		}
	}
}

func seedDB(t *testing.T) {
	for i := 0; i < 10; i++ {
		payload, _ := json.Marshal(map[string]string{"value": fmt.Sprintf("value-%d", i)})
		resp, err := http.Post(fmt.Sprintf("http://db:8082/db/%d", i), "application/json", bytes.NewReader(payload))
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("Failed to seed key %d: %v, status: %d", i, err, resp.StatusCode)
		}
		resp.Body.Close()
	}
}
