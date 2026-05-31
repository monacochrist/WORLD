package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/stripe/stripe-go/v76"
	"github.com/stripe/stripe-go/v76/checkout/session"
	"github.com/stripe/stripe-go/v76/webhook"
)

type Button struct {
	Text      string  `json:"text"`
	HoverText string  `json:"hover_text"`
	URL       string  `json:"url"`
	Enabled   bool    `json:"enabled"`
	X         float64 `json:"x"`
	Y         float64 `json:"y"`
}

type AudioTrack struct {
	URL  string  `json:"url"`
	Loop bool    `json:"loop"`
	X    float64 `json:"x"`
	Y    float64 `json:"y"`
}

type Config struct {
	Title       string       `json:"title"`
	BgColor     string       `json:"bg_color"`
	TitleColor  string       `json:"title_color"`
	Buttons     []Button     `json:"buttons"`
	AudioTracks []AudioTrack `json:"audio_tracks"`
}

type UserData struct {
	Balance         int `json:"balance"`
	SecondsListened int `json:"seconds_listened"`
}

type SSEBroker struct {
	mu      sync.RWMutex
	clients map[chan []byte]struct{}
}

func NewSSEBroker() *SSEBroker {
	return &SSEBroker{clients: make(map[chan []byte]struct{})}
}

func (b *SSEBroker) Subscribe() chan []byte {
	ch := make(chan []byte, 10)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *SSEBroker) Unsubscribe(ch chan []byte) {
	b.mu.Lock()
	delete(b.clients, ch)
	b.mu.Unlock()
}

func (b *SSEBroker) Broadcast(data []byte) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.clients {
		select {
		case ch <- data:
		default:
		}
	}
}

type BalanceStore struct {
	mu       sync.Mutex
	path     string
	balances map[string]UserData
}

func NewBalanceStore(path string) *BalanceStore {
	return &BalanceStore{path: path}
}

func (s *BalanceStore) Lock()   { s.mu.Lock() }
func (s *BalanceStore) Unlock() { s.mu.Unlock() }

func (s *BalanceStore) Load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			s.balances = make(map[string]UserData)
			return nil
		}
		return err
	}
	var result map[string]UserData
	if err := json.Unmarshal(data, &result); err == nil {
		s.balances = result
		return nil
	}
	var old map[string]int
	if err := json.Unmarshal(data, &old); err != nil {
		return err
	}
	s.balances = make(map[string]UserData)
	for k, v := range old {
		s.balances[k] = UserData{Balance: v}
	}
	return nil
}

func (s *BalanceStore) Save() error {
	data, err := json.MarshalIndent(s.balances, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0644)
}

func (s *BalanceStore) Get(email string) UserData {
	return s.balances[email]
}

func (s *BalanceStore) Set(email string, u UserData) {
	s.balances[email] = u
}

var (
	configMu   sync.Mutex
	rateLimitMu sync.Mutex
	lastTokenUse = make(map[string]time.Time)
)

func checkTokenRate(email string) bool {
	rateLimitMu.Lock()
	defer rateLimitMu.Unlock()
	last, ok := lastTokenUse[email]
	now := time.Now()
	if ok && now.Sub(last) < 10*time.Second {
		return false
	}
	lastTokenUse[email] = now
	return true
}

func loadConfig() (*Config, error) {
	data, err := os.ReadFile("config.json")
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func saveConfig(cfg *Config) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile("config.json", data, 0644)
}

func main() {
	stripe.Key = os.Getenv("STRIPE_SECRET_KEY")
	webhookSecret := os.Getenv("STRIPE_WEBHOOK_SECRET")
	editSecret := os.Getenv("EDIT_SECRET")
	siteDomain := os.Getenv("SITE_DOMAIN")
	if siteDomain == "" {
		siteDomain = "http://localhost:8080"
	}

	broker := NewSSEBroker()
	store := NewBalanceStore("balances.json")

	http.HandleFunc("/", indexHandler(editSecret))
	http.HandleFunc("/events", eventsHandler(broker))
	http.HandleFunc("/save-config", saveConfigHandler(editSecret, broker))
	http.HandleFunc("/api/balance", balanceHandler(store))
	http.HandleFunc("/api/use-tokens", useTokensHandler(store))
	http.HandleFunc("/buy-tokens", buyTokensHandler(store, siteDomain))
	http.HandleFunc("/success", successHandler(store))
	if webhookSecret != "" {
		http.HandleFunc("/stripe-webhook", stripeWebhookHandler(store, webhookSecret))
	}

	log.Println("Listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func indexHandler(secret string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		configMu.Lock()
		cfg, err := loadConfig()
		configMu.Unlock()
		if err != nil {
			http.Error(w, "Failed to load config", http.StatusInternalServerError)
			log.Printf("config error: %v", err)
			return
		}
		isEdit := secret != "" && r.URL.Query().Get("edit") == secret
		cfgJSON, _ := json.Marshal(cfg)
		tmpl := template.Must(template.ParseFiles("templates/index.html"))
		tmpl.Execute(w, map[string]interface{}{
			"Config":     cfg,
			"ConfigJSON": template.JS(cfgJSON),
			"IsEdit":     isEdit,
			"EditSecret": secret,
		})
	}
}

func eventsHandler(broker *SSEBroker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		ch := broker.Subscribe()
		defer broker.Unsubscribe(ch)

		configMu.Lock()
		cfg, err := loadConfig()
		configMu.Unlock()
		if err == nil {
			data, _ := json.Marshal(cfg)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}

		for {
			select {
			case msg := <-ch:
				fmt.Fprintf(w, "data: %s\n\n", msg)
				flusher.Flush()
			case <-r.Context().Done():
				return
			}
		}
	}
}

func saveConfigHandler(secret string, broker *SSEBroker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("edit") != secret {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		bodyBytes, _ := io.ReadAll(r.Body)
		var cfg Config
		if err := json.Unmarshal(bodyBytes, &cfg); err != nil {
			log.Printf("save-config JSON error: %v | body: %s", err, string(bodyBytes))
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		configMu.Lock()
		err := saveConfig(&cfg)
		configMu.Unlock()
		if err != nil {
			http.Error(w, "Failed to save", http.StatusInternalServerError)
			log.Printf("save error: %v", err)
			return
		}
		data, _ := json.Marshal(cfg)
		broker.Broadcast(data)
		w.WriteHeader(http.StatusOK)
	}
}

func balanceHandler(store *BalanceStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		email := r.URL.Query().Get("email")
		if email == "" {
			http.Error(w, "email required", http.StatusBadRequest)
			return
		}
		store.Lock()
		if err := store.Load(); err != nil {
			store.Unlock()
			http.Error(w, "Error", http.StatusInternalServerError)
			return
		}
		user := store.Get(email)
		store.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"email":            email,
			"balance":          user.Balance,
			"seconds_listened": user.SecondsListened,
		})
	}
}

func useTokensHandler(store *BalanceStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Email  string `json:"email"`
			Amount int    `json:"amount"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Email == "" || body.Amount <= 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid request"})
			return
		}
		if !checkTokenRate(body.Email) {
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]string{"error": "rate limited"})
			return
		}
		store.Lock()
		defer store.Unlock()
		if err := store.Load(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "server error"})
			return
		}
		user := store.Get(body.Email)
		if user.Balance < body.Amount {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"balance":          user.Balance,
				"seconds_listened": user.SecondsListened,
				"ok":               false,
			})
			return
		}
		user.Balance -= body.Amount
		user.SecondsListened += 30
		store.Set(body.Email, user)
		store.Save()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"balance":          user.Balance,
			"seconds_listened": user.SecondsListened,
			"ok":               true,
		})
	}
}

func buyTokensHandler(store *BalanceStore, domain string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if stripe.Key == "" {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "Stripe not configured"})
			return
		}

		var body struct {
			Email string `json:"email"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Email == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "email required"})
			return
		}

		params := &stripe.CheckoutSessionParams{
			Mode:          stripe.String(string(stripe.CheckoutSessionModePayment)),
			SuccessURL:    stripe.String(domain + "/success?session_id={CHECKOUT_SESSION_ID}"),
			CancelURL:     stripe.String(domain + "/"),
			CustomerEmail: stripe.String(body.Email),
			LineItems: []*stripe.CheckoutSessionLineItemParams{
				{
					PriceData: &stripe.CheckoutSessionLineItemPriceDataParams{
						Currency: stripe.String("usd"),
						ProductData: &stripe.CheckoutSessionLineItemPriceDataProductDataParams{
							Name: stripe.String("500 Tokens"),
						},
						UnitAmount: stripe.Int64(500),
					},
					Quantity: stripe.Int64(1),
				},
			},
		}

		s, err := session.New(params)
		if err != nil {
			log.Printf("stripe session error: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "Stripe error"})
			return
		}

		json.NewEncoder(w).Encode(map[string]string{"url": s.URL})
	}
}

func successHandler(store *BalanceStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sessionID := r.URL.Query().Get("session_id")
		if sessionID == "" {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}

		s, err := session.Get(sessionID, nil)
		if err != nil {
			log.Printf("stripe session get error: %v", err)
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}

		if s.PaymentStatus != stripe.CheckoutSessionPaymentStatusPaid {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}

		if s.CustomerEmail == "" {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}

		store.Lock()
		if err := store.Load(); err != nil {
			store.Unlock()
			http.Error(w, "Error", http.StatusInternalServerError)
			return
		}
		user := store.Get(s.CustomerEmail)
		user.Balance += 500
		store.Set(s.CustomerEmail, user)
		store.Save()
		balance := user.Balance
		secondsListened := user.SecondsListened
		store.Unlock()

		tmpl := template.Must(template.ParseFiles("templates/success.html"))
		tmpl.Execute(w, map[string]interface{}{
			"Email":            s.CustomerEmail,
			"Balance":          balance,
			"SecondsListened":  secondsListened,
		})
	}
}

func stripeWebhookHandler(store *BalanceStore, webhookSecret string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const MaxBodyBytes = int64(65536)
		r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading body", http.StatusServiceUnavailable)
			return
		}

		sigHeader := r.Header.Get("Stripe-Signature")
		event, err := webhook.ConstructEvent(payload, sigHeader, webhookSecret)
		if err != nil {
			log.Printf("webhook signature error: %v", err)
			http.Error(w, "Signature verification failed", http.StatusBadRequest)
			return
		}

		if event.Type == "checkout.session.completed" {
			var s stripe.CheckoutSession
			if err := json.Unmarshal(event.Data.Raw, &s); err != nil {
				log.Printf("webhook parse error: %v", err)
				http.Error(w, "Error parsing session", http.StatusBadRequest)
				return
			}

			if s.CustomerEmail != "" {
				store.Lock()
				if err := store.Load(); err != nil {
					store.Unlock()
					http.Error(w, "Error", http.StatusInternalServerError)
					return
				}
				user := store.Get(s.CustomerEmail)
				user.Balance += 500
				store.Set(s.CustomerEmail, user)
				store.Save()
				store.Unlock()
				log.Printf("webhook: credited 500 tokens to %s", s.CustomerEmail)
			}
		}

		w.WriteHeader(http.StatusOK)
	}
}
