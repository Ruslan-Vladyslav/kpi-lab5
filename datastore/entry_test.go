package datastore

import (
	"bytes"
	"testing"
)

func TestSerializeDeserialize(t *testing.T) {
	input := kvPair{"key", "value"}
	data := Serialize(input)
	result, err := LoadEntry(bytes.NewReader(data), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.key != input.key {
		t.Errorf("expected key %q, got %q", input.key, result.key)
	}
	if result.value != input.value {
		t.Errorf("expected value %q, got %q", input.value, result.value)
	}
}
