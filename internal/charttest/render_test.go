//go:build helm

package charttest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

type port struct {
	Port          int
	ContainerPort int `yaml:"containerPort"`
}
type container struct {
	Args  []string
	Ports []port
}
type manifest struct {
	Kind string
	Spec struct {
		Ports    []port
		Template struct {
			Metadata struct{ Annotations map[string]string }
			Spec     struct{ Containers []container }
		}
	}
}

func render(t *testing.T, app string, values map[string]any) (map[string]manifest, error) {
	t.Helper()
	helm := os.Getenv("HELM")
	if helm == "" {
		helm = "helm"
	}
	cmd := exec.CommandContext(t.Context(), helm, "template", "check", "../../"+app+"/charts/"+app, "--values", "-")
	data, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdin = bytes.NewReader(data)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	raw, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, stderr.String())
	}
	docs := map[string]manifest{}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	for {
		var doc manifest
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			return docs, nil
		}
		if err != nil {
			t.Fatal(err)
		}
		docs[doc.Kind] = doc
	}
}

func TestRenderedDaemonArguments(t *testing.T) {
	apps := []struct {
		name, key string
		input     any
		args      []string
	}{
		{"capensis", "pds", []string{"a.example", "b.example"}, []string{"--pds=a.example", "--pds=b.example"}},
		{"helix", "accounts", []string{"a.example", "b.example"}, []string{"--account=a.example", "--account=b.example"}},
		{"cordata", "post", "at://did:example:a/app.bsky.feed.post/key", []string{"at://did:example:a/app.bsky.feed.post/key"}},
		{"trifolium", "accounts", []string{"a.example", "b.example"}, []string{"--accounts=a.example,b.example"}},
	}
	for _, app := range apps {
		t.Run(app.name, func(t *testing.T) {
			values := map[string]any{"identifier": "owner.example", "existingSecret": "secret", app.key: app.input}
			for _, identifier := range []any{"123", 123} {
				values["identifier"] = identifier
				docs, err := render(t, app.name, values)
				if err != nil {
					t.Fatal(err)
				}
				deploy := docs["Deployment"]
				if len(deploy.Spec.Template.Spec.Containers) != 1 {
					t.Fatal("expected one daemon")
				}
				args := deploy.Spec.Template.Spec.Containers[0].Args
				for _, want := range append(slices.Clone(app.args), "--identifier=123") {
					if !slices.Contains(args, want) {
						t.Fatalf("missing %q in %v", want, args)
					}
				}
				for _, arg := range args {
					if strings.Contains(arg, "%!") {
						t.Fatalf("malformed argument %q", arg)
					}
				}
			}
			for _, required := range []string{"identifier", "existingSecret", app.key} {
				saved := values[required]
				delete(values, required)
				if _, err := render(t, app.name, values); err == nil {
					t.Errorf("accepted missing %s", required)
				}
				values[required] = saved
			}
		})
	}
}

func TestMetricsPortsAgree(t *testing.T) {
	for _, app := range []string{"helix", "capensis"} {
		for _, n := range []int{9090, 18128} {
			values := map[string]any{"identifier": "owner.example", "existingSecret": "secret", "pds": []string{"a.example"}, "accounts": []string{"a.example"}, "metrics": map[string]any{"enabled": true, "port": n}}
			docs, err := render(t, app, values)
			if err != nil {
				t.Fatal(err)
			}
			pod := docs["Deployment"].Spec.Template
			c := pod.Spec.Containers[0]
			if !slices.Contains(c.Args, "--metrics-address=:"+strconv.Itoa(n)) || len(c.Ports) != 1 || c.Ports[0].ContainerPort != n || pod.Metadata.Annotations["prometheus.io/port"] != strconv.Itoa(n) {
				t.Fatalf("%s pod metrics mismatch for %d", app, n)
			}
			service := docs["Service"]
			if len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != n {
				t.Fatalf("%s Service port mismatch", app)
			}
		}
	}
}

func TestDefaultClusterHasOneSource(t *testing.T) {
	for mask := 0; mask < 8; mask++ {
		values := map[string]any{"identifier": "owner.example", "existingSecret": "secret"}
		clusters := map[string]any{}
		for i, id := range []string{"default", "garden"} {
			if mask&(1<<i) != 0 {
				clusters[id] = map[string]any{"accounts": []string{"a.example"}, "threshold": 1}
			}
		}
		values["clusters"] = clusters
		if mask&4 != 0 {
			values["accounts"] = []string{"a.example"}
		}
		docs, err := render(t, "trifolium", values)
		valid := mask != 0 && mask&5 != 5
		if (err == nil) != valid {
			t.Fatalf("sources mask %d: %v", mask, err)
		}
		if valid && len(clusters) > 0 {
			raw, _ := json.Marshal(clusters)
			if !slices.Contains(docs["Deployment"].Spec.Template.Spec.Containers[0].Args, "--clusters="+string(raw)) {
				t.Fatalf("lost cluster settings %s", raw)
			}
		}
	}
}

func TestNumericIntervalsRenderAsSeconds(t *testing.T) {
	for _, app := range []string{"capensis", "helix", "cordata", "trifolium"} {
		for _, value := range []any{6, "6"} {
			docs, err := render(t, app, map[string]any{
				"identifier": "owner.example", "existingSecret": "secret", "accounts": []string{"a.example"}, "pds": []string{"a.example"},
				"post": "at://did:example:a/app.bsky.feed.post/key", "writeInterval": value, "rescanInterval": value,
			})
			if err != nil {
				t.Fatal(err)
			}
			args := docs["Deployment"].Spec.Template.Spec.Containers[0].Args
			for _, flag := range []string{"write-interval", "rescan-interval"} {
				if !slices.Contains(args, "--"+flag+"=6s") {
					t.Errorf("%s rendered %s without seconds: %v", app, flag, args)
				}
			}
		}
	}
}

func TestClusterValuesAgreeWithDaemon(t *testing.T) {
	for _, scope := range []string{"legacy", "named"} {
		for _, accounts := range [][]string{{}, {"  "}, {"a.example"}} {
			for _, threshold := range []int{-1, 0, 1, 3} {
				values := map[string]any{"identifier": "owner.example", "existingSecret": "secret"}
				if scope == "legacy" {
					values["accounts"] = accounts
					values["threshold"] = threshold
				} else {
					values["clusters"] = map[string]any{"garden": map[string]any{"accounts": accounts, "threshold": threshold}}
				}
				_, err := render(t, "trifolium", values)
				want := threshold > 0 && len(accounts) > 0 && strings.TrimSpace(accounts[0]) != ""
				if (err == nil) != want {
					t.Errorf("%s accounts=%q threshold=%d: %v", scope, accounts, threshold, err)
				}
			}
		}
	}
}
