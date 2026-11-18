package palustre

import (
	"bytes"
	"encoding/binary"
	"image"
	"image/color"
	"image/png"
	"math/rand"
	"runtime"
	"testing"
)

func icoTestPNG(t *testing.T, w, h int, seed int64) []byte {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	img := image.NewNRGBA(image.Rect(0, 0, w, h))
	base := color.NRGBA{R: uint8(rng.Intn(256)), G: uint8(rng.Intn(256)), B: uint8(rng.Intn(256)), A: 255}
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.Set(x, y, base)
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

type icoTestEntry struct {
	payload []byte
	w, h    int
	// offset >= 0 forces the entry to share payload bytes already in the
	// file (overlapping entries), instead of appending its own copy.
	offset int
	size   int
}

func buildTestICO(entries []icoTestEntry) []byte {
	data := make([]byte, 6+16*len(entries))
	copy(data, []byte{0, 0, 1, 0})
	binary.LittleEndian.PutUint16(data[4:6], uint16(len(entries)))
	offset := 6 + 16*len(entries)
	for i, entry := range entries {
		dir := data[6+i*16 : 6+(i+1)*16]
		dir[0], dir[1] = byte(entry.w%256), byte(entry.h%256)
		at, size := entry.offset, entry.size
		if at < 0 {
			at, size = offset, len(entry.payload)
			offset += len(entry.payload)
		}
		binary.LittleEndian.PutUint32(dir[8:12], uint32(size))
		binary.LittleEndian.PutUint32(dir[12:16], uint32(at))
	}
	for _, entry := range entries {
		if entry.offset < 0 {
			data = append(data, entry.payload...)
		}
	}
	return data
}

func icoEntry(payload []byte, w, h int) icoTestEntry {
	return icoTestEntry{payload: payload, w: w, h: h, offset: -1}
}

// Regression: a multi-entry ICO whose largest entry is corrupt was
// rejected outright even when a smaller entry decoded fine.
func TestNormalizeIconFallsBackToValidICOEntry(t *testing.T) {
	small := icoTestPNG(t, 16, 16, 21)
	large := icoTestPNG(t, 64, 32, 22)
	corruptLarge := append([]byte("not an image"), make([]byte, 64)...)
	ico := buildTestICO([]icoTestEntry{icoEntry(small, 16, 16), icoEntry(corruptLarge, 64, 32)})
	out, err := normalizeIcon(ico)
	if err != nil {
		t.Fatalf("valid small entry ignored: %v", err)
	}
	cfg, _, err := image.DecodeConfig(bytes.NewReader(out))
	if err != nil || cfg.Width != 16 || cfg.Height != 16 {
		t.Fatalf("fallback produced %dx%d, err %v; want 16x16", cfg.Width, cfg.Height, err)
	}
	ico = buildTestICO([]icoTestEntry{icoEntry(small, 16, 16), icoEntry(large, 64, 32)})
	out, err = normalizeIcon(ico)
	if err != nil {
		t.Fatal(err)
	}
	cfg, _, _ = image.DecodeConfig(bytes.NewReader(out))
	if cfg.Width != 64 || cfg.Height != 32 {
		t.Fatalf("largest valid entry not preferred: %dx%d", cfg.Width, cfg.Height)
	}
	ico = buildTestICO([]icoTestEntry{icoEntry(corruptLarge, 16, 16), icoEntry(corruptLarge, 64, 32)})
	if _, err := normalizeIcon(ico); err == nil {
		t.Fatal("ICO with only corrupt entries accepted")
	}
}

// Regression: the ICO codec panics on a truncated BMP entry (its forged
// BMP header reads past the payload). That panic must fail only that
// entry, not the whole icon, when another entry is usable.
func TestNormalizeIconFallsBackWhenICOEntryPanicsCodec(t *testing.T) {
	small := icoTestPNG(t, 16, 16, 31)
	panicking := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	ico := buildTestICO([]icoTestEntry{icoEntry(small, 16, 16), icoEntry(panicking, 64, 64)})
	out, err := normalizeIcon(ico)
	if err != nil {
		t.Fatalf("panicking largest entry discarded a usable icon: %v", err)
	}
	cfg, _, err := image.DecodeConfig(bytes.NewReader(out))
	if err != nil || cfg.Width != 16 || cfg.Height != 16 {
		t.Fatalf("fallback produced %dx%d, err %v; want 16x16", cfg.Width, cfg.Height, err)
	}
	// A panicking entry on its own is still an error, never a crash.
	ico = buildTestICO([]icoTestEntry{icoEntry(panicking, 64, 64)})
	if _, err := normalizeIcon(ico); err == nil {
		t.Fatal("panicking sole entry accepted")
	}
}

// The directory may name 256 entries overlapping one 4 MiB payload.
// Keeping every payload copy would expand that to ~1 GiB; the decoder
// must retain only offsets and materialize one entry at a time.
func TestNormalizeIconOverlappingEntriesStayBounded(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 4<<20)
	header := 6 + 16*256
	entries := make([]icoTestEntry, 256)
	for i := range entries {
		entries[i] = icoTestEntry{w: 256, h: 256, offset: header, size: len(payload)}
	}
	data := buildTestICO(entries)
	data = append(data, payload...)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if _, err := normalizeIcon(data); err == nil {
		t.Fatal("256 corrupt overlapping entries accepted")
	}
	runtime.ReadMemStats(&after)
	if delta := after.TotalAlloc - before.TotalAlloc; delta > 64<<20 {
		t.Fatalf("normalizing overlapping entries allocated %d bytes; want bounded by one entry, not all 256", delta)
	}
}

// Property: generated ICO directories with mixed valid, corrupt, and
// panicking entries either return the largest decodable entry as a
// bounded PNG or an error, and never panic.
func TestPropertyICOEntryFallback(t *testing.T) {
	rng := rand.New(rand.NewSource(17))
	valid := icoTestPNG(t, 24, 24, 41)
	coverage := map[string]int{}
	for trial := 0; trial < 200; trial++ {
		n := 1 + rng.Intn(5)
		entries := make([]icoTestEntry, n)
		anyValid := false
		for i := range entries {
			switch rng.Intn(3) {
			case 0:
				entries[i] = icoEntry(valid, 24, 24)
				anyValid = true
				coverage["valid"]++
			case 1:
				entries[i] = icoEntry([]byte{1, 2, 3}, 32, 32)
				coverage["panicking"]++
			default:
				entries[i] = icoEntry([]byte("garbage-payload"), 16, 16)
				coverage["corrupt"]++
			}
		}
		out, err := normalizeIcon(buildTestICO(entries))
		if anyValid {
			if err != nil {
				t.Fatalf("trial %d: valid entry present but icon rejected: %v", trial, err)
			}
			cfg, format, err := image.DecodeConfig(bytes.NewReader(out))
			if err != nil || format != "png" || cfg.Width != 24 || cfg.Height != 24 {
				t.Fatalf("trial %d: bad fallback output %dx%d %q %v", trial, cfg.Width, cfg.Height, format, err)
			}
		} else if err == nil {
			t.Fatalf("trial %d: no valid entry but icon accepted", trial)
		}
	}
	for _, kind := range []string{"valid", "panicking", "corrupt"} {
		if coverage[kind] == 0 {
			t.Fatalf("generator never produced %s entries: %v", kind, coverage)
		}
	}
	t.Logf("ico fallback coverage: %v", coverage)
}
