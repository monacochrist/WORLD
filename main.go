package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/stripe/stripe-go/v76"
	"github.com/stripe/stripe-go/v76/checkout/session"
)

type Button struct {
	Text      string `json:"text"`
	HoverText string `json:"hover_text"`
	URL       string `json:"url"`
	Enabled   bool   `json:"enabled"`
	X         int    `json:"x"`
	Y         int    `json:"y"`
}

type Config struct {
	Title      string   `json:"title"`
	BgColor    string   `json:"bg_color"`
	TitleColor string   `json:"title_color"`
	Buttons    []Button `json:"buttons"`
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

func loadBalances() (map[string]int, error) {
	data, err := os.ReadFile("balances.json")
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]int), nil
		}
		return nil, err
	}
	balances := make(map[string]int)
	if err := json.Unmarshal(data, &balances); err != nil {
		return nil, err
	}
	return balances, nil
}

func saveBalances(balances map[string]int) error {
	data, err := json.MarshalIndent(balances, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile("balances.json", data, 0644)
}

func main() {
	stripe.Key = os.Getenv("STRIPE_SECRET_KEY")
	editSecret := os.Getenv("EDIT_SECRET")
	broker := NewSSEBroker()

	http.HandleFunc("/", indexHandler(editSecret))
	http.HandleFunc("/events", eventsHandler(broker))
	http.HandleFunc("/save-config", saveConfigHandler(editSecret, broker))
	http.HandleFunc("/api/balance", balanceHandler)
	http.HandleFunc("/buy-tokens", buyTokensHandler)
	http.HandleFunc("/success", successHandler)

	log.Println("Listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func indexHandler(secret string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cfg, err := loadConfig()
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

		cfg, err := loadConfig()
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
		var cfg Config
		if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		if err := saveConfig(&cfg); err != nil {
			http.Error(w, "Failed to save", http.StatusInternalServerError)
			log.Printf("save error: %v", err)
			return
		}
		data, _ := json.Marshal(cfg)
		broker.Broadcast(data)
		w.WriteHeader(http.StatusOK)
	}
}

func balanceHandler(w http.ResponseWriter, r *http.Request) {
	email := r.URL.Query().Get("email")
	if email == "" {
		http.Error(w, "email required", http.StatusBadRequest)
		return
	}
	balances, err := loadBalances()
	if err != nil {
		http.Error(w, "Error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"email":   email,
		"balance": balances[email],
	})
}

func buyTokensHandler(w http.ResponseWriter, r *http.Request) {
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

	domain := "http://localhost:8080"
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

func successHandler(w http.ResponseWriter, r *http.Request) {
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

	balances, err := loadBalances()
	if err != nil {
		http.Error(w, "Error", http.StatusInternalServerError)
		return
	}
	balances[s.CustomerEmail] += 500
	saveBalances(balances)

	tmpl := template.Must(template.ParseFiles("templates/success.html"))
	tmpl.Execute(w, map[string]interface{}{
		"Email":   s.CustomerEmail,
		"Balance": balances[s.CustomerEmail],
	})
}
