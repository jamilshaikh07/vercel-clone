// Diagnostic: mint a GitHub App installation access token outside the
// cluster so we can verify the App identity is configured correctly.
// Usage:
//   go run . -key /path/to/private-key.pem -app-id 3908003 -install 136626167
package main

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	keyPath := flag.String("key", "", "path to App private key PEM")
	appID := flag.Int64("app-id", 0, "GitHub App ID")
	installation := flag.Int64("install", 0, "installation ID")
	flag.Parse()

	if *keyPath == "" || *appID == 0 || *installation == 0 {
		flag.Usage()
		os.Exit(2)
	}

	pemB, err := os.ReadFile(*keyPath)
	check(err)
	block, _ := pem.Decode(pemB)
	if block == nil {
		fmt.Fprintln(os.Stderr, "pem decode failed")
		os.Exit(1)
	}
	var key *rsa.PrivateKey
	switch block.Type {
	case "RSA PRIVATE KEY":
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	default:
		anyKey, err2 := x509.ParsePKCS8PrivateKey(block.Bytes)
		check(err2)
		key = anyKey.(*rsa.PrivateKey)
	}
	check(err)

	now := time.Now()
	hb, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	pb, _ := json.Marshal(map[string]any{
		"iat": now.Add(-30 * time.Second).Unix(),
		"exp": now.Add(9 * time.Minute).Unix(),
		"iss": *appID,
	})
	enc := base64.RawURLEncoding
	signingInput := enc.EncodeToString(hb) + "." + enc.EncodeToString(pb)
	sum := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	check(err)
	jwt := signingInput + "." + enc.EncodeToString(sig)

	url := fmt.Sprintf("https://api.github.com/app/installations/%d/access_tokens", *installation)
	req, _ := http.NewRequestWithContext(context.Background(), "POST", url, nil)
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := http.DefaultClient.Do(req)
	check(err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Fprintf(os.Stderr, "status: %d\n", resp.StatusCode)
	fmt.Fprintf(os.Stderr, "body  : %s\n", string(body))
	if resp.StatusCode/100 != 2 {
		os.Exit(1)
	}
	var r struct {
		Token       string         `json:"token"`
		ExpiresAt   string         `json:"expires_at"`
		Permissions map[string]any `json:"permissions"`
	}
	_ = json.Unmarshal(body, &r)
	fmt.Fprintf(os.Stderr, "perms : %v\n", r.Permissions)
	fmt.Println(r.Token)
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
