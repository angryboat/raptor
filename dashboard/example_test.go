package dashboard_test

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/angryboat/raptor/dashboard"
	"github.com/jackc/pgx/v5/pgxpool"
)

func requireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "admin" || pass != os.Getenv("DASHBOARD_PASSWORD") {
			w.Header().Set("WWW-Authenticate", `Basic realm="raptor"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func ExampleNew() {
	pool, err := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	mux := http.NewServeMux()
	mux.Handle("/raptor/", http.StripPrefix("/raptor", requireAdmin(dashboard.New(pool))))

	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}
