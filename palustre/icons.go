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
	"net/http"
	"net/url"
	"strings"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	_ "github.com/fyne-io/image/ico"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"golang.org/x/image/draw"
	_ "golang.org/x/image/webp"
	"golang.org/x/net/html"

	"github.com/alyraffauf/terrarium/palustre/listlex"
)

const maxIconDownload = 4 << 20

func (api *Session) fetchIconBytes(ctx context.Context, endpoint string, limit int64) ([]byte, *url.URL, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, nil, err
	}
	response, err := api.HTTP.Do(request)
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
	data, _, err := api.fetchIconBytes(ctx, endpoint, maxIconDownload)
	if err != nil {
		return nil, err
	}
	return normalizeIcon(data)
}

func normalizeIcon(data []byte) (normalized []byte, err error) {
	// The ICO codec can panic on malformed bitmap headers. A bad remote icon must not stop the worker.
	defer func() {
		if failure := recover(); failure != nil {
			normalized = nil
			err = fmt.Errorf("invalid icon: %v", failure)
		}
	}()
	// Decode only one ICO entry, bounding its allocation before handing it to the codec.
	if len(data) >= 6 && bytes.Equal(data[:4], []byte{0, 0, 1, 0}) {
		count := int(binary.LittleEndian.Uint16(data[4:6]))
		if count == 0 || count > 256 || len(data) < 6+16*count {
			return nil, fmt.Errorf("invalid ICO directory")
		}
		best, area := -1, 0
		for i := 0; i < count; i++ {
			entry := data[6+i*16 : 6+(i+1)*16]
			width, height := int(entry[0]), int(entry[1])
			if width == 0 {
				width = 256
			}
			if height == 0 {
				height = 256
			}
			if width*height > area {
				best, area = i, width*height
			}
		}
		entry := append([]byte(nil), data[6+best*16:6+(best+1)*16]...)
		size, offset := uint64(binary.LittleEndian.Uint32(entry[8:12])), uint64(binary.LittleEndian.Uint32(entry[12:16]))
		if size == 0 || offset+size > uint64(len(data)) {
			return nil, fmt.Errorf("invalid ICO image bounds")
		}
		binary.LittleEndian.PutUint32(entry[12:16], 22)
		selected := append([]byte{0, 0, 1, 0, 1, 0}, entry...)
		data = append(selected, data[offset:offset+size]...)
	}
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
	page, base, err := api.fetchIconBytes(ctx, pds, 1<<20)
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
		data, fetchErr := api.FetchIcon(ctx, endpoint)
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
	uri, err := syntax.ParseATURI(listURI)
	if err != nil {
		return err
	}
	record, err := atproto.RepoGetRecord(ctx, api.Client, "", ListCollection, api.Client.Auth.Did, uri.RecordKey().String())
	if err != nil {
		return err
	}
	if record.Value == nil {
		return fmt.Errorf("managed list has no record value")
	}
	list, ok := record.Value.Val.(*listlex.GraphList)
	if !ok || list.Name != name || list.Purpose == nil || *list.Purpose != "app.bsky.graph.defs#modlist" {
		return fmt.Errorf("managed list changed identity")
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
	_, err = atproto.RepoPutRecord(ctx, api.Client, &atproto.RepoPutRecord_Input{Repo: api.Client.Auth.Did, Collection: ListCollection, Rkey: uri.RecordKey().String(), Record: &lexutil.LexiconTypeDecoder{Val: list}, SwapRecord: record.Cid})
	api.ObserveWriteError(err)
	return err
}
