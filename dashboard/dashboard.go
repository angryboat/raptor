// Package dashboard serves a read-only HTML view of a raptor database: queue
// depths and lifetime counts, running, retrying and dead jobs, and schedules
// with their next run.
package dashboard

import (
	"bytes"
	_ "embed"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"time"

	"github.com/angryboat/raptor"
)

//go:embed dashboard.html
var page string

var tmpl = template.Must(template.New("dashboard").Funcs(template.FuncMap{
	"at":       newMoment,
	"num":      formatCount,
	"duration": formatDuration,
	"iso":      formatISO,
}).Parse(page))

// Handler serves the dashboard. It does no authentication or authorization,
// so wrap it in your own middleware before exposing it.
type Handler struct {
	db raptor.DB
}

// New returns a Handler that reads from db. Pass a *pgxpool.Pool rather than
// a *pgx.Conn, since requests can arrive concurrently and a connection runs
// one query at a time.
func New(db raptor.DB) *Handler {
	return &Handler{db: db}
}

// ServeHTTP renders the dashboard for GET and HEAD requests to any path and
// rejects other methods. While its Live toggle is on, the page refreshes its
// data every 5 seconds from the URL it was loaded from, so mount it with
// http.StripPrefix under whatever path you like.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	snap, err := load(r.Context(), h.db)
	if err != nil {
		http.Error(w, "raptor: dashboard: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, view{snapshot: snap, Limit: listLimit}); err != nil {
		http.Error(w, "raptor: dashboard: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = buf.WriteTo(w)
}

type view struct {
	snapshot
	Limit int
}

type moment struct {
	ISO      string
	Relative string
}

func newMoment(now, t time.Time) moment {
	return moment{ISO: formatISO(t), Relative: formatRelative(now, t)}
}

func formatISO(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

func formatRelative(now, t time.Time) string {
	d := t.Sub(now).Round(time.Second)
	switch {
	case d == 0:
		return "now"
	case d > 0:
		return "in " + formatSpan(d)
	default:
		return formatSpan(-d) + " ago"
	}
}

var spanUnits = []struct {
	size   time.Duration
	suffix string
}{
	{24 * time.Hour, "d"},
	{time.Hour, "h"},
	{time.Minute, "m"},
	{time.Second, "s"},
}

func formatSpan(d time.Duration) string {
	for i, u := range spanUnits {
		if d < u.size {
			continue
		}
		s := fmt.Sprintf("%d%s", d/u.size, u.suffix)
		if i+1 < len(spanUnits) {
			next := spanUnits[i+1]
			if rest := d % u.size / next.size; rest > 0 {
				s += fmt.Sprintf(" %d%s", rest, next.suffix)
			}
		}
		return s
	}
	return "0s"
}

func formatDuration(d time.Duration) string {
	if d < time.Second {
		return strconv.FormatInt(d.Milliseconds(), 10) + "ms"
	}
	return formatSpan(d.Round(time.Second))
}

func formatCount(n int64) string {
	if n < 0 {
		return "-" + formatCount(-n)
	}
	s := strconv.FormatInt(n, 10)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}
