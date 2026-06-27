package ui

import (
	"bytes"
	"embed"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"
)

//go:embed dist
var distFS embed.FS

func NewHandler() http.Handler {
	files, err := fs.Sub(distFS, "dist/app")
	if err == nil {
		if _, err := fs.Stat(files, "index.html"); err == nil {
			return spaHandler{files: files}
		}
	}
	return fallbackHandler{}
}

type spaHandler struct {
	files fs.FS
}

type fallbackHandler struct{}

func (fallbackHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if strings.HasPrefix(path.Clean(r.URL.Path), "/assets/") {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write([]byte(`<!doctype html><html lang="ja"><head><meta charset="UTF-8"><title>Mahiron</title></head><body><main><h1>Mahiron WebUI is not built</h1><p>Run <code>make web-build</code> to embed the WebUI.</p></main></body></html>`))
}

func (h spaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
	if name == "." || name == "" {
		name = "index.html"
	}

	info, err := fs.Stat(h.files, name)
	if err != nil || info.IsDir() {
		name = "index.html"
		info, err = fs.Stat(h.files, name)
		if err != nil {
			http.NotFound(w, r)
			return
		}
	}

	data, err := fs.ReadFile(h.files, name)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	if contentType := mime.TypeByExtension(path.Ext(name)); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	} else {
		w.Header().Set("Content-Type", http.DetectContentType(data))
	}
	if strings.HasPrefix(name, "assets/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "no-cache")
	}

	http.ServeContent(w, r, name, info.ModTime(), bytes.NewReader(data))
}
