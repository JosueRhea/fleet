// Package tclient dials Temporal from env: TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_API_KEY.
// Without an API key it targets a local dev server (`temporal server start-dev`).
package tclient

import (
	"crypto/tls"
	"os"

	"go.temporal.io/sdk/client"
)

func Dial() (client.Client, error) {
	opts := client.Options{
		HostPort:  getenv("TEMPORAL_ADDRESS", "localhost:7233"),
		Namespace: getenv("TEMPORAL_NAMESPACE", "default"),
	}
	if key := os.Getenv("TEMPORAL_API_KEY"); key != "" {
		opts.Credentials = client.NewAPIKeyStaticCredentials(key)
		opts.ConnectionOptions = client.ConnectionOptions{TLS: &tls.Config{}}
	}
	return client.Dial(opts)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
