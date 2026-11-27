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
	endpoint.ForceQuery = false
	return endpoint.String(), nil
}

// NormalizeJetstream converts http(s) spellings to ws(s) and canonicalizes
// the endpoint for a Jetstream v2 subscription. An explicit default port
// and an omitted port are the same endpoint, so both spellings produce
// the identical string: the stream checkpoint is keyed by it. The result
// is the full StreamPath URL with a canonical host and no query/fragment.
func NormalizeJetstream(raw string) (string, error) {
	if prefix := strings.ToLower(raw); strings.HasPrefix(prefix, "https://") {
		raw = "wss://" + raw[len("https://"):]
	} else if strings.HasPrefix(prefix, "http://") {
		raw = "ws://" + raw[len("http://"):]
	}
	stream, err := url.Parse(raw)
	if err != nil || stream.Host == "" || (stream.Scheme != "ws" && stream.Scheme != "wss") || stream.User != nil {
		return "", fmt.Errorf("jetstream must be a ws:// or wss:// URL")
	}
	host, err := canonicalHost(stream, true)
	if err != nil {
		return "", fmt.Errorf("jetstream must be a ws:// or wss:// URL")
	}
	stream.Host = host
	if stream.Path == "" || stream.Path == "/" {
		stream.Path = StreamPath
	}
	if stream.Path != StreamPath || stream.RawQuery != "" || stream.Fragment != "" {
		return "", fmt.Errorf("jetstream must be a base URL or the v2 subscribeEvents URL without query parameters")
	}
	stream.ForceQuery = false
	return stream.String(), nil
}

// StreamEndpointsEqual reports whether two stored or configured endpoint
// strings name the same stream. Spellings that both canonicalize are
// compared canonically, so a checkpoint saved before canonicalization
// (an explicit default port, a leading-zero port, a differently cased
// host) is recovered rather than abandoned. Strings that do not parse
// as Jetstream endpoints fall back to exact equality.
func StreamEndpointsEqual(a, b string) bool {
	if a == b {
		return true
	}
	ca, errA := NormalizeJetstream(a)
	cb, errB := NormalizeJetstream(b)
	return errA == nil && errB == nil && ca == cb
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
