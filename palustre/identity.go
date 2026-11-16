package palustre

import (
	"fmt"
	"net/url"
	"strings"
)

func NormalizePDS(raw string) (string, error) {
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	endpoint, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if endpoint.Hostname() == "" || (endpoint.Scheme != "https" && endpoint.Scheme != "http") || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || strings.Trim(endpoint.Path, "/") != "" {
		return "", fmt.Errorf("invalid PDS base URL %q", raw)
	}
	endpoint.Host = strings.ToLower(endpoint.Host)
	if endpoint.Port() == "443" && endpoint.Scheme == "https" || endpoint.Port() == "80" && endpoint.Scheme == "http" {
		endpoint.Host = endpoint.Hostname()
		if strings.Contains(endpoint.Host, ":") {
			endpoint.Host = "[" + endpoint.Host + "]"
		}
	}
	endpoint.Path = ""
	return endpoint.String(), nil
}
