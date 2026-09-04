//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	namespace    = "taskmole"
	endpoint     = "http://127.0.0.1:18082"
	managerImage = "example.com/taskmole:v0.0.1"
	fixtureImage = "example.com/taskmole-fixture:v0.0.1"
)

func TestDurableMCPInvocation(t *testing.T) {
	root := projectRoot(t)
	run(t, root, "task", "container-build", "IMG="+managerImage)
	loadImage(t, root, managerImage)
	run(t, root, "task", "fixture-build", "FIXTURE_IMG="+fixtureImage)
	loadImage(t, root, fixtureImage)
	run(t, root, "kubectl", "create", "namespace", namespace)
	t.Cleanup(func() { _, _ = command(root, "kubectl", "delete", "namespace", namespace, "--ignore-not-found") })
	run(t, root, "kubectl", "label", "--overwrite", "namespace", namespace, "pod-security.kubernetes.io/enforce=restricted")
	run(t, root, "task", "install", "IMG="+managerImage)
	run(t, root, "kubectl", "rollout", "status", "deployment/taskmole-manager", "-n", namespace, "--timeout=3m")
	run(t, root, "kubectl", "apply", "-f", "test/fixture/profile.yaml")
	waitFor(t, time.Minute, func() bool {
		out, _ := command(root, "kubectl", "get", "workerprofile", "fixture", "-n", namespace, "-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
		return out == "True"
	})

	forward := exec.Command("kubectl", "port-forward", "-n", namespace, "service/taskmole-mcp", "18082:8082")
	forward.Dir = root
	if err := forward.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = forward.Process.Kill(); _, _ = forward.Process.Wait() })
	waitFor(t, time.Minute, func() bool {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:18082", time.Second)
		if err == nil {
			_ = conn.Close()
		}
		return err == nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	session := connect(t, ctx)
	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(tools.Tools))
	for _, tool := range tools.Tools {
		names = append(names, tool.Name)
	}
	slices.Sort(names)
	wantTools := []string{"get_worker_task", "invoke_worker", "list_running_worker_tasks", "list_worker_tasks", "list_workers"}
	if !reflect.DeepEqual(names, wantTools) {
		t.Fatalf("tools = %v, want %v", names, wantTools)
	}
	workers := call[workerList](t, ctx, session, "list_workers", map[string]any{})
	if len(workers.Workers) != 1 || workers.Workers[0].Name != "fixture" {
		t.Fatalf("workers = %#v", workers)
	}

	successID := invoke(t, ctx, session, "success", "durable", "success-key")
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	fresh := connect(t, ctx)
	defer fresh.Close()
	if replay := invoke(t, ctx, fresh, "success", "durable", "success-key"); replay != successID {
		t.Fatalf("replay ID = %s, want %s", replay, successID)
	}
	success := awaitPhase(t, ctx, fresh, successID, "Succeeded", 2*time.Minute)
	assertJSON(t, success.Result, `{"acknowledged":true,"message":"durable"}`)
	pod, _ := command(root, "kubectl", "get", "workertask", successID, "-n", namespace, "-o", "jsonpath={.status.podName}")
	run(t, root, "kubectl", "delete", "pod", pod, "-n", namespace, "--wait=true")
	assertJSON(t, getTask(t, ctx, fresh, successID).Result, `{"acknowledged":true,"message":"durable"}`)

	failure := awaitPhase(t, ctx, fresh, invoke(t, ctx, fresh, "failure", "fail", "failure-key"), "Failed", time.Minute)
	if failure.ExitCode == nil || *failure.ExitCode != 7 {
		t.Fatalf("exit code = %v", failure.ExitCode)
	}
	for _, tc := range []struct {
		mode, phase, reason string
		timeout             time.Duration
	}{{"invalid", "Failed", "InvalidJSON", time.Minute}, {"missing", "Failed", "MissingResult", time.Minute}, {"timeout", "TimedOut", "DeadlineExceeded", 90 * time.Second}} {
		got := awaitPhase(t, ctx, fresh, invoke(t, ctx, fresh, tc.mode, tc.mode, tc.mode+"-key"), tc.phase, tc.timeout)
		if got.Reason != tc.reason {
			t.Fatalf("%s reason = %s", tc.mode, got.Reason)
		}
	}
	run(t, root, "kubectl", "delete", "workertask", successID, "-n", namespace)
	waitFor(t, time.Minute, func() bool {
		job, _ := command(root, "kubectl", "get", "job", successID, "-n", namespace, "--ignore-not-found", "-o", "name")
		input, _ := command(root, "kubectl", "get", "configmap", successID+"-input", "-n", namespace, "--ignore-not-found", "-o", "name")
		return job == "" && input == ""
	})
}

type workerList struct {
	Workers []struct {
		Name                      string `json:"name"`
		InputSchema, OutputSchema map[string]any
	} `json:"workers"`
}
type taskResult struct {
	Phase, Reason string
	ExitCode      *int32          `json:"exitCode"`
	Result        json.RawMessage `json:"result"`
}

func connect(t *testing.T, ctx context.Context) *mcp.ClientSession {
	t.Helper()
	session, err := mcp.NewClient(&mcp.Implementation{Name: "taskmole-e2e", Version: "v1"}, nil).Connect(ctx, &mcp.StreamableClientTransport{Endpoint: endpoint, DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return session
}
func invoke(t *testing.T, ctx context.Context, s *mcp.ClientSession, mode, message, key string) string {
	return call[struct {
		TaskID string `json:"taskId"`
	}](t, ctx, s, "invoke_worker", map[string]any{"profileName": "fixture", "idempotencyKey": key, "input": map[string]any{"mode": mode, "message": message}}).TaskID
}
func getTask(t *testing.T, ctx context.Context, s *mcp.ClientSession, id string) taskResult {
	return call[taskResult](t, ctx, s, "get_worker_task", map[string]any{"taskId": id})
}
func awaitPhase(t *testing.T, ctx context.Context, s *mcp.ClientSession, id, phase string, timeout time.Duration) taskResult {
	t.Helper()
	var got taskResult
	waitFor(t, timeout, func() bool { got = getTask(t, ctx, s, id); return got.Phase == phase })
	return got
}
func call[T any](t *testing.T, ctx context.Context, s *mcp.ClientSession, name string, args any) T {
	t.Helper()
	result, err := s.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("%s failed: %#v", name, result.Content)
	}
	raw, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func waitFor(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatal("timed out waiting for condition")
}
func run(t *testing.T, dir, name string, args ...string) {
	t.Helper()
	if out, err := command(dir, name, args...); err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
}
func command(dir, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "KUBECTL_KUBERC=false")
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}
func projectRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(wd, "../.."))
}
func loadImage(t *testing.T, root, image string) {
	t.Helper()
	tool := os.Getenv("CONTAINER_TOOL")
	if tool == "" {
		tool = "podman"
	}
	archive := filepath.Join(t.TempDir(), "image.tar")
	run(t, root, tool, "save", "--output", archive, image)
	kind := os.Getenv("KIND")
	if kind == "" {
		kind = "kind"
	}
	cluster := os.Getenv("KIND_CLUSTER")
	if cluster == "" {
		cluster = "taskmole-test-e2e"
	}
	run(t, root, kind, "load", "image-archive", archive, "--name", cluster)
}
func assertJSON(t *testing.T, got json.RawMessage, want string) {
	t.Helper()
	var a, b any
	if json.Unmarshal(got, &a) != nil || json.Unmarshal([]byte(want), &b) != nil || !reflect.DeepEqual(a, b) {
		t.Fatalf("JSON = %s, want %s", got, want)
	}
}
