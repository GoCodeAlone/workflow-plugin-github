package internal

import (
	"net/http"

	"github.com/google/go-github/v69/github"
)

// SDKClient wraps the go-github client with auth support.
type SDKClient struct {
	GH *github.Client
}

// NewSDKClient creates a client authenticated with a personal access token.
func NewSDKClient(token string) *SDKClient {
	client := github.NewClient(nil).WithAuthToken(token)
	return &SDKClient{GH: client}
}

// NewSDKClientFromTransport creates a client from an http.RoundTripper (for GitHub App auth).
func NewSDKClientFromTransport(rt http.RoundTripper) *SDKClient {
	httpClient := &http.Client{Transport: rt}
	client := github.NewClient(httpClient)
	return &SDKClient{GH: client}
}
