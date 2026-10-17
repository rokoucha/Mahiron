package agent

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestUploadReportsBoundedRedactedRejection(t *testing.T) {
	const token = "test-agent-secret"
	for _, tc := range []struct{ name, body, want string }{
		{"parser failure", "Error when parsing request", "Error when parsing request"},
		{"credential reflection", "rejected " + token, "rejected [redacted]"},
		{"oversized response", strings.Repeat("x", 5000), strings.Repeat("x", 4096)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				http.Error(w, tc.body, http.StatusBadRequest)
			}))
			defer server.Close()
			base, err := url.Parse(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "input.ts")
			if err := os.WriteFile(path, tsData(), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err = receiveAndUpload(ctx, base, token, "generation", "session", Request{Command: "cat '" + path + "'", Transport: "ts", TimeoutMS: 1000})
			want := "stream rejected: HTTP 400: " + tc.want
			if err == nil || err.Error() != want {
				t.Fatalf("error = %v, want %q", err, want)
			}
		})
	}
}
