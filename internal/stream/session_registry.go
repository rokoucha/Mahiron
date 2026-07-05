package stream

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/observability"
)

type sessionKey struct {
	channel string
	typ     string
}

type sessionEntry struct {
	session   Session
	routeType string
	source    string
	startedAt time.Time
}

type sessionCreate struct {
	done    chan struct{}
	err     error
	session Session
}

// sessionRegistry tracks live stream sessions and coordinates the
// in-flight deduplication of concurrent create requests for the same key.
type sessionRegistry struct {
	mu           sync.Mutex
	sessions     map[sessionKey]sessionEntry
	creates      map[sessionKey]*sessionCreate
	shuttingDown bool
}

func newSessionRegistry() *sessionRegistry {
	return &sessionRegistry{
		sessions: map[sessionKey]sessionEntry{},
		creates:  map[sessionKey]*sessionCreate{},
	}
}

// acquireResult reports the outcome of an acquire call. Exactly one of its
// fields is meaningful: an existing entry, a pending create to wait on, a
// freshly registered create the caller must fulfil, or a shutting-down signal.
type acquireResult struct {
	entry        sessionEntry
	hasEntry     bool
	pending      *sessionCreate
	create       *sessionCreate
	shuttingDown bool
}

// acquire atomically resolves a session request: it reuses a live session,
// joins an in-flight create, rejects new work during shutdown, or registers a
// new create that the caller must complete via completeCreate.
func (r *sessionRegistry) acquire(key sessionKey) acquireResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.sessions[key]; ok {
		return acquireResult{entry: entry, hasEntry: true}
	}
	if r.shuttingDown {
		return acquireResult{shuttingDown: true}
	}
	if create, ok := r.creates[key]; ok {
		return acquireResult{pending: create}
	}
	create := &sessionCreate{done: make(chan struct{})}
	r.creates[key] = create
	return acquireResult{create: create}
}

// completeCreate records the result of a create started by acquire. On success
// the session is added to the registry; either way waiters are released.
func (r *sessionRegistry) completeCreate(key sessionKey, create *sessionCreate, session Session, routeType, source string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err == nil {
		r.addLocked(key, session, routeType, source)
	}
	create.session = session
	create.err = err
	delete(r.creates, key)
	close(create.done)
}

func (r *sessionRegistry) has(key sessionKey) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.sessions[key]
	return ok
}

func (r *sessionRegistry) get(key sessionKey) (Session, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.sessions[key]
	return entry.session, ok
}

func (r *sessionRegistry) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sessions)
}

func (r *sessionRegistry) remove(key sessionKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeLocked(key)
}

// removeIfSame evicts the registry entry for key only if it still holds the
// given session, so a caller that observed a stale/dead session never evicts
// a newer one that has since replaced it.
func (r *sessionRegistry) removeIfSame(key sessionKey, session Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.sessions[key]; ok && entry.session == session {
		r.removeLocked(key)
	}
}

// beginShutdown marks the registry as shutting down and returns a snapshot of
// the in-flight creates so callers can wait for them to settle.
func (r *sessionRegistry) beginShutdown() []*sessionCreate {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shuttingDown = true
	creates := make([]*sessionCreate, 0, len(r.creates))
	for _, create := range r.creates {
		creates = append(creates, create)
	}
	return creates
}

func (r *sessionRegistry) activeSessions() []Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	sessions := make([]Session, 0, len(r.sessions))
	for _, entry := range r.sessions {
		sessions = append(sessions, entry.session)
	}
	return sessions
}

func (r *sessionRegistry) clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range r.sessions {
		r.removeLocked(key)
	}
}

func (r *sessionRegistry) addLocked(key sessionKey, session Session, routeType, source string) {
	r.sessions[key] = sessionEntry{
		session:   session,
		routeType: routeType,
		source:    source,
		startedAt: time.Now(),
	}
}

func (r *sessionRegistry) removeLocked(key sessionKey) {
	entry, ok := r.sessions[key]
	if !ok {
		return
	}
	r.recordDurationLocked(key, entry)
	delete(r.sessions, key)
}

func (r *sessionRegistry) recordDurationLocked(key sessionKey, entry sessionEntry) {
	if entry.startedAt.IsZero() {
		return
	}
	observability.RecordStreamSessionDuration(context.Background(), key.typ, entry.routeType, entry.source, time.Since(entry.startedAt).Milliseconds())
}

func waitSessionCreates(ctx context.Context, creates []*sessionCreate) error {
	var result error
	for _, create := range creates {
		select {
		case <-create.done:
		case <-ctx.Done():
			result = errors.Join(result, ctx.Err())
		}
	}
	return result
}
