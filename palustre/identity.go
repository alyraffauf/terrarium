package palustre

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
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
	host, err := canonicalHost(endpoint, true)
	if err != nil {
		return "", fmt.Errorf("invalid PDS base URL %q", raw)
	}
	endpoint.Host = host
	endpoint.Path = ""
	return endpoint.String(), nil
}

// canonicalHost renders the host of endpoint in one spelling: lower-case
// DNS names, net.IP spellings for literals, and a numeric port with no
// leading zeros. Ports outside 1-65535 are rejected. When stripDefault is
// set, the scheme's default port (443 for https/wss, 80 for http/ws) is
// dropped; otherwise an explicit port is kept.
func canonicalHost(endpoint *url.URL, stripDefault bool) (string, error) {
	hostname := strings.ToLower(endpoint.Hostname())
	if ip := net.ParseIP(hostname); ip != nil {
		hostname = ip.String()
	}
	host := hostname
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	port := endpoint.Port()
	if port == "" {
		return host, nil
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return "", fmt.Errorf("invalid port %q", port)
	}
	isDefault := (endpoint.Scheme == "https" || endpoint.Scheme == "wss") && number == 443 ||
		(endpoint.Scheme == "http" || endpoint.Scheme == "ws") && number == 80
	if stripDefault && isDefault {
		return host, nil
	}
	return host + ":" + strconv.Itoa(number), nil
}
