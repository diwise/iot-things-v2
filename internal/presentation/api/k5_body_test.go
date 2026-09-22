package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
)

// TV-08: överstor HTTP-body nekas innan allokering (K5).
func TestDecodeBodyRejectsOversize(t *testing.T) {
	big := strings.Repeat("x", maxRequestBodyBytes+100)
	body := map[string]any{"template": map[string]any{"id": big}}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatalf("encode: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/x", &buf)
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(v0ctx("t"))
	if _, err := decodeBody[catalog.TemplateSpec](req); err == nil {
		t.Fatal("oversize body must be rejected")
	}
}

func TestDecodeThingSpecRejectsOversize(t *testing.T) {
	big := strings.Repeat("x", maxRequestBodyBytes+100)
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"thingId":"a","name":"`+big+`"}`))
	req.Header.Set("Content-Type", "application/json")
	if _, err := decodeThingSpec(req); err == nil {
		t.Fatal("oversize thing body must be rejected")
	}
}
