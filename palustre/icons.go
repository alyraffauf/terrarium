package palustre

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	"image/png"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	_ "github.com/fyne-io/image/ico"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"golang.org/x/image/draw"
	_ "golang.org/x/image/webp"
	"golang.org/x/net/html"

	"github.com/bluesky-social/indigo/api/bsky"
)

const maxIconDownload = 4 << 20

// Icon URLs found in remote content (a page's icon link, a profile
// avatar field, a redirect target) are not operator configuration, so
// they may only reach public addresses. The one exception is the host
// of the operator-configured source that produced them, which stays
// reachable even on a private network (a self-hosted PDS, a test
// server). Resolution is rechecked at dial time so a name cannot pass
// validation and then resolve somewhere private.
func isPublicIP(ip net.IP) bool {
	if ip == nil || !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return false
	}
	for _, prefix := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "198.18.0.0/15"} {
		address, ok := netip.AddrFromSlice(ip)
		if ok && netip.MustParsePrefix(prefix).Contains(address.Unmap()) {
			return false
		}
	}
	return true
}

func sameIconHost(host, trusted string) bool {
	if trusted == "" {
		return false
	}
	if strings.EqualFold(host, trusted) {
		return true
	}
	a, b := net.ParseIP(host), net.ParseIP(trusted)
	return a != nil && b != nil && a.Equal(b)
}

func checkIconTarget(ctx context.Context, target *url.URL, trustedHost string) error {
	if (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" || target.User != nil {
		return fmt.Errorf("icon URL must be an http(s) URL without credentials")
	}
	host := target.Hostname()
	if sameIconHost(host, trustedHost) {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil {
		if !isPublicIP(ip) {
			return fmt.Errorf("icon URL host %q is not a public address", host)
		}
		return nil
	}
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
	if err != nil {
		return err
	}
	for _, ip := range ips {
		if !isPublicIP(ip) {
			return fmt.Errorf("icon URL host %q resolves to a non-public address", host)
		}
	}
	return nil
}

func (api *Session) iconHTTP(trustedHost string) *http.Client {
	base := api.HTTP
	if base == nil {
		base = http.DefaultClient
	}
	client := *base
	var transport *http.Transport
	switch current := base.Transport.(type) {
	case nil:
		transport = http.DefaultTransport.(*http.Transport).Clone()
	case *http.Transport:
		transport = current.Clone()
	default:
		transport = http.DefaultTransport.(*http.Transport).Clone()
	}
	if transport != nil {
		// Direct dialing keeps destination checks effective even when a
		// proxy or custom TLS dialer is configured on the shared client.
		transport.Proxy = nil
		transport.DialTLSContext = nil
		transport.DialTLS = nil
		dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
		dial := dialer.DialContext
		transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			if sameIconHost(host, trustedHost) {
				return dial(ctx, network, address)
			}
			ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
			if err != nil {
				return nil, err
			}
			var lastErr error = fmt.Errorf("icon host %q has no addresses", host)
			for _, ip := range ips {
				if !isPublicIP(ip) {
					return nil, fmt.Errorf("icon host %q resolves to a non-public address", host)
				}
				conn, err := dial(ctx, network, net.JoinHostPort(ip.String(), port))
				if err == nil {
					return conn, nil
				}
				lastErr = err
			}
			return nil, lastErr
		}
		client.Transport = transport
	}
	previous := base.CheckRedirect
	client.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return fmt.Errorf("icon fetch stopped after 10 redirects")
		}
		if previous != nil {
			if err := previous(request, via); err != nil {
				return err
			}
		}
		return checkIconTarget(request.Context(), request.URL, trustedHost)
	}
	return &client
}

func (api *Session) fetchIconBytes(ctx context.Context, endpoint string, limit int64, trustedHost string) ([]byte, *url.URL, error) {
	target, err := url.Parse(endpoint)
	if err != nil {
		return nil, nil, err
	}
	if err := checkIconTarget(ctx, target, trustedHost); err != nil {
		return nil, nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, nil, err
	}
	client := api.iconHTTP(trustedHost)
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	if err != nil {
		return nil, nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("icon fetch returned HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, nil, err
	}
	if int64(len(data)) > limit {
		return nil, nil, fmt.Errorf("icon response exceeds %d bytes", limit)
	}
	return data, response.Request.URL, nil
}

// FetchIcon converts supported raster formats to a bounded PNG suitable for list avatars.
func (api *Session) FetchIcon(ctx context.Context, endpoint string) ([]byte, error) {
	return api.fetchIcon(ctx, endpoint, "")
}

func (api *Session) fetchIcon(ctx context.Context, endpoint, trustedHost string) ([]byte, error) {
	data, _, err := api.fetchIconBytes(ctx, endpoint, maxIconDownload, trustedHost)
	if err != nil {
		return nil, err
	}
	return normalizeIcon(data)
}

// icoCandidate locates one ICO entry without copying its payload. A
// directory may name 256 entries overlapping the same bytes; retaining
// every payload would expand a bounded download by orders of magnitude.
type icoCandidate struct {
	area   int
	size   uint32
	offset uint32
	entry  [16]byte
}

func icoCandidates(data []byte) ([]icoCandidate, error) {
	count := int(binary.LittleEndian.Uint16(data[4:6]))
	if count == 0 || count > 256 || len(data) < 6+16*count {
		return nil, fmt.Errorf("invalid ICO directory")
	}
	var candidates []icoCandidate
	for i := 0; i < count; i++ {
		var candidate icoCandidate
		copy(candidate.entry[:], data[6+i*16:6+(i+1)*16])
		width, height := int(candidate.entry[0]), int(candidate.entry[1])
		if width == 0 {
			width = 256
		}
		if height == 0 {
			height = 256
		}
		candidate.area = width * height
		candidate.size = binary.LittleEndian.Uint32(candidate.entry[8:12])
		candidate.offset = binary.LittleEndian.Uint32(candidate.entry[12:16])
		if candidate.size == 0 || uint64(candidate.offset)+uint64(candidate.size) > uint64(len(data)) {
			continue
		}
		candidates = append(candidates, candidate)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("invalid ICO image bounds")
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].area > candidates[j].area })
	return candidates, nil
}

// plausibleICOEntry reports whether a payload can be handed to the ICO
// codec: a PNG, or a BMP DIB long enough for the codec's forged header
// (it reads 36 bytes unconditionally and panics on shorter payloads).
func plausibleICOEntry(payload []byte) bool {
	if bytes.HasPrefix(payload, []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}) {
		return true
	}
	if len(payload) < 40 {
		return false
	}
	switch dib := binary.LittleEndian.Uint32(payload[:4]); dib {
	case 12, 16, 40, 52, 56, 108, 124:
		return int(dib) <= len(payload)
	}
	return false
}

func normalizeIcon(data []byte) ([]byte, error) {
	// Decode ICO entries largest first, and fall back when an entry is
	// corrupt. Only offsets are retained from the directory; a single
	// entry is materialized at decode time.
	if len(data) >= 6 && bytes.Equal(data[:4], []byte{0, 0, 1, 0}) {
		candidates, err := icoCandidates(data)
		if err != nil {
			return nil, err
		}
		var lastErr error
		for _, candidate := range candidates {
			payload := data[candidate.offset : candidate.offset+candidate.size]
			if !plausibleICOEntry(payload) {
				lastErr = fmt.Errorf("invalid ICO entry payload")
				continue
			}
			entry := candidate.entry
			binary.LittleEndian.PutUint32(entry[12:16], 22)
			selected := append([]byte{0, 0, 1, 0, 1, 0}, entry[:]...)
			selected = append(selected, payload...)
			rendered, renderErr := renderIcon(selected)
			if renderErr == nil {
				return rendered, nil
			}
			lastErr = renderErr
		}
		return nil, lastErr
	}
	return renderIcon(data)
}

// renderIcon renders one image. The ICO codec can panic on malformed
// bitmap headers, so a panic is recovered per image: one bad entry must
// neither stop the worker nor discard other entries of the same file.
func renderIcon(data []byte) (normalized []byte, err error) {
	defer func() {
		if failure := recover(); failure != nil {
			normalized = nil
			err = fmt.Errorf("invalid icon: %v", failure)
		}
	}()
	config, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	if config.Width <= 0 || config.Height <= 0 || config.Width > 4096 || config.Height > 4096 {
		return nil, fmt.Errorf("icon dimensions exceed 4096 pixels")
	}
	source, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	width, height := config.Width, config.Height
	if max(width, height) > 256 {
		scale := float64(256) / float64(max(width, height))
		width, height = max(1, int(float64(width)*scale)), max(1, int(float64(height)*scale))
	}
	target := image.NewNRGBA(image.Rect(0, 0, width, height))
	draw.CatmullRom.Scale(target, target.Bounds(), source, source.Bounds(), draw.Src, nil)
	var output bytes.Buffer
	if err := png.Encode(&output, target); err != nil {
		return nil, err
	}
	if output.Len() > 1_000_000 {
		return nil, fmt.Errorf("icon exceeds list avatar size limit")
	}
	return output.Bytes(), nil
}

// Favicon tries declared icons before the conventional /favicon.ico location.
func (api *Session) Favicon(ctx context.Context, pds string) ([]byte, error) {
	candidates := []string{}
	source, _ := url.Parse(pds)
	trustedHost := ""
	if source != nil {
		trustedHost = source.Hostname()
	}
	page, base, err := api.fetchIconBytes(ctx, pds, 1<<20, trustedHost)
	if err == nil {
		tokenizer := html.NewTokenizer(bytes.NewReader(page))
		for len(candidates) < 16 {
			kind := tokenizer.Next()
			if kind == html.ErrorToken {
				break
			}
			if kind != html.StartTagToken && kind != html.SelfClosingTagToken {
				continue
			}
			token := tokenizer.Token()
			if token.Data != "link" {
				continue
			}
			var rel, href string
			for _, attr := range token.Attr {
				if attr.Key == "rel" {
					rel = strings.ToLower(attr.Val)
				}
				if attr.Key == "href" {
					href = attr.Val
				}
			}
			isIcon := false
			for _, value := range strings.Fields(rel) {
				if value == "icon" || value == "apple-touch-icon" {
					isIcon = true
				}
			}
			if !isIcon || href == "" {
				continue
			}
			reference, err := url.Parse(href)
			if err != nil {
				continue
			}
			resolved := base.ResolveReference(reference)
			if resolved.Scheme == "http" || resolved.Scheme == "https" {
				candidates = append(candidates, resolved.String())
			}
		}
	}
	candidates = append(candidates, strings.TrimRight(pds, "/")+"/favicon.ico")
	for _, endpoint := range candidates {
		data, fetchErr := api.fetchIcon(ctx, endpoint, trustedHost)
		if fetchErr == nil {
			return data, nil
		}
		err = fetchErr
		if ctx.Err() != nil {
			break
		}
	}
	return nil, err
}

// UpdateListIcon preserves the rest of the record and uses its CID to prevent lost edits.
func (api *Session) UpdateListIcon(ctx context.Context, listURI, name string, data []byte) error {
	key, err := api.listRecordKey(listURI)
	if err != nil {
		return err
	}
	record, err := atproto.RepoGetRecord(ctx, api.Client, "", ListCollection, api.Client.Auth.Did, key)
	if err != nil {
		return err
	}
	if record.Value == nil {
		return fmt.Errorf("managed list has no record value")
	}
	list, ok := record.Value.Val.(*bsky.GraphList)
	if !ok || list.Name != name || list.Purpose == nil || *list.Purpose != moderationListPurpose {
		return fmt.Errorf("%w: managed list changed identity", ErrUnrelatedList)
	}
	digest, err := multihash.Sum(data, multihash.SHA2_256, -1)
	if err != nil {
		return err
	}
	expected := cid.NewCidV1(cid.Raw, digest)
	if list.Avatar != nil && cid.Cid(list.Avatar.Ref).Equals(expected) {
		return nil
	}
	if record.Cid == nil || *record.Cid == "" {
		return fmt.Errorf("managed list has no CID")
	}
	if err := api.WaitWrite(ctx); err != nil {
		return err
	}
	var uploaded atproto.RepoUploadBlob_Output
	err = api.Client.LexDo(ctx, lexutil.Procedure, "image/png", "com.atproto.repo.uploadBlob", nil, bytes.NewReader(data), &uploaded)
	api.ObserveWriteError(err)
	if err != nil {
		return err
	}
	if uploaded.Blob == nil || !cid.Cid(uploaded.Blob.Ref).Equals(expected) {
		return fmt.Errorf("uploaded icon has unexpected CID")
	}
	list.Avatar = uploaded.Blob
	if err := api.WaitWrite(ctx); err != nil {
		return err
	}
	_, err = atproto.RepoPutRecord(ctx, api.Client, &atproto.RepoPutRecord_Input{Repo: api.Client.Auth.Did, Collection: ListCollection, Rkey: key, Record: &lexutil.LexiconTypeDecoder{Val: list}, SwapRecord: record.Cid})
	api.ObserveWriteError(err)
	return err
}
