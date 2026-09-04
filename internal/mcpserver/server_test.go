package mcpserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	taskmolev1alpha1 "github.com/andreasvikke/taskmole/api/v1alpha1"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	extensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testNamespace   = "workers"
	testProfileName = "echo"
)

func TestListWorkersOverStreamableHTTP(t *testing.T) {
	server := testServer(t, testProfile("ready", true))
	httpServer := httptest.NewServer(server.HTTP.Handler)
	t.Cleanup(httpServer.Close)
	mcpClient := mcp.NewClient(&mcp.Implementation{Name: "contract-test", Version: "v1"}, nil)
	session, err := mcpClient.Connect(t.Context(), &mcp.StreamableClientTransport{
		Endpoint: httpServer.URL, DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "list_workers", Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("list_workers returned a tool error: %#v", result.Content)
	}
}

func TestOAuthDiscoveryReturnsNotFound(t *testing.T) {
	server := testServer(t)
	request := httptest.NewRequest(http.MethodGet, "/.well-known/oauth-protected-resource", nil)
	response := httptest.NewRecorder()

	server.HTTP.Handler.ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body = %q", response.Code, http.StatusNotFound, response.Body.String())
	}
}

func TestMCPPathAcceptsStreamableHTTP(t *testing.T) {
	server := testServer(t, testProfile("ready", true))
	httpServer := httptest.NewServer(server.HTTP.Handler)
	t.Cleanup(httpServer.Close)
	client := mcp.NewClient(&mcp.Implementation{Name: "path-test", Version: "v1"}, nil)
	session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{
		Endpoint: httpServer.URL + "/mcp", DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	result, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Tools) != 5 {
		t.Fatalf("tools = %d, want 5", len(result.Tools))
	}
}

func TestInitializeResponse(t *testing.T) {
	server := testServer(t)
	body := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"remote-client","version":"1"}}}`
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	response := httptest.NewRecorder()

	server.HTTP.Handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", contentType)
	}
	var message struct {
		Result struct {
			ProtocolVersion string `json:"protocolVersion"`
		} `json:"result"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &message); err != nil {
		t.Fatalf("decode initialization response: %v", err)
	}
	if message.Result.ProtocolVersion != "2025-06-18" {
		t.Fatalf("protocol version = %q, want 2025-06-18", message.Result.ProtocolVersion)
	}
}

func TestListWorkersRedactsRuntimeAndDefaultsToReady(t *testing.T) {
	ready := testProfile("ready", true)
	ready.Spec.Runtime.Env = nil
	unavailable := testProfile("unavailable", false)
	server := testServer(t, ready, unavailable)

	_, output, err := server.listWorkers(context.Background(), nil, listWorkersInput{})
	if err != nil {
		t.Fatal(err)
	}
	if len(output.Workers) != 1 || output.Workers[0].Name != "ready" {
		t.Fatalf("workers = %#v", output.Workers)
	}
	encoded, err := json.Marshal(output)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"runtime", "image", "envFrom", "serviceAccountName"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("discovery leaked %q: %s", secret, encoded)
		}
	}

	_, output, err = server.listWorkers(context.Background(), nil, listWorkersInput{IncludeUnavailable: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(output.Workers) != 2 || output.Workers[1].UnavailableReason != taskmolev1alpha1.ReasonInvalidSchema {
		t.Fatalf("workers including unavailable = %#v", output.Workers)
	}
}

func TestInvokeAndGetTaskUseDurableState(t *testing.T) {
	server := testServer(t, testProfile(testProfileName, true))
	_, invoked, err := server.invokeWorker(context.Background(), nil, invokeWorkerInput{
		ProfileName: testProfileName, IdempotencyKey: "caller-request-1", Input: map[string]any{"message": "hello"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// A fresh interface instance can retrieve the task using only its durable ID.
	fresh := New("127.0.0.1:0", testNamespace, server.Client)
	_, got, err := fresh.getWorkerTask(context.Background(), nil, getTaskInput(invoked))
	if err != nil {
		t.Fatal(err)
	}
	rawInput, err := json.Marshal(got.Input)
	if err != nil {
		t.Fatal(err)
	}
	if got.TaskID != invoked.TaskID || string(rawInput) != `{"message":"hello"}` || got.ProfileName != testProfileName {
		t.Fatalf("task = %#v", got)
	}
}

func TestInvokeIsIdempotentAndRejectsMismatches(t *testing.T) {
	server := testServer(t, testProfile(testProfileName, true))
	ctx := context.Background()
	request := invokeWorkerInput{ProfileName: testProfileName, IdempotencyKey: "same-key", Input: map[string]any{"message": "one"}}
	_, first, err := server.invokeWorker(ctx, nil, request)
	if err != nil {
		t.Fatal(err)
	}
	_, replay, err := server.invokeWorker(ctx, nil, request)
	if err != nil || replay.TaskID != first.TaskID {
		t.Fatalf("replay = %#v, error = %v", replay, err)
	}
	request.Input = map[string]any{"message": "two"}
	if _, _, err := server.invokeWorker(ctx, nil, request); err == nil || !strings.HasPrefix(err.Error(), "idempotency_conflict:") {
		t.Fatalf("mismatch error = %v", err)
	}
}

func TestInvokeMapsDomainErrorsToStableCodes(t *testing.T) {
	server := testServer(t, testProfile("unready", false))
	tests := []struct {
		name  string
		input invokeWorkerInput
		code  string
	}{
		{"missing profile", invokeWorkerInput{ProfileName: "missing", IdempotencyKey: "1", Input: map[string]any{}}, "profile_not_found:"},
		{"unready profile", invokeWorkerInput{ProfileName: "unready", IdempotencyKey: "2", Input: map[string]any{}}, "profile_unavailable:"},
		{"invalid input", invokeWorkerInput{ProfileName: "unready", IdempotencyKey: "", Input: map[string]any{}}, "invalid_input:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := server.invokeWorker(context.Background(), nil, tt.input)
			if err == nil || !strings.HasPrefix(err.Error(), tt.code) {
				t.Fatalf("error = %v, want code %s", err, tt.code)
			}
		})
	}
}

func TestTaskListsReturnCompactSummariesAndFilterRunning(t *testing.T) {
	pending := testTask("pending", taskmolev1alpha1.WorkerTaskPhasePending)
	done := testTask("done", taskmolev1alpha1.WorkerTaskPhaseSucceeded)
	server := testServer(t, pending, done)
	_, all, err := server.listWorkerTasks(context.Background(), nil, listTasksInput{})
	if err != nil || len(all.Tasks) != 2 {
		t.Fatalf("all = %#v, error = %v", all, err)
	}
	_, running, err := server.listRunningWorkerTasks(context.Background(), nil, listTasksInput{})
	if err != nil || len(running.Tasks) != 1 || running.Tasks[0].TaskID != "pending" {
		t.Fatalf("running = %#v, error = %v", running, err)
	}
	encoded, _ := json.Marshal(all)
	if strings.Contains(string(encoded), "input") || strings.Contains(string(encoded), "result") {
		t.Fatalf("list was not compact: %s", encoded)
	}
}

func testServer(t *testing.T, objects ...client.Object) *Server {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := taskmolev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return New("127.0.0.1:0", testNamespace, fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build())
}

func testProfile(name string, ready bool) *taskmolev1alpha1.WorkerProfile {
	status := metav1.ConditionFalse
	reason := taskmolev1alpha1.ReasonInvalidSchema
	if ready {
		status, reason = metav1.ConditionTrue, taskmolev1alpha1.ReasonValid
	}
	return &taskmolev1alpha1.WorkerProfile{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace, Generation: 1},
		Spec: taskmolev1alpha1.WorkerProfileSpec{DisplayName: name, Description: "test worker", Tags: []string{"test"},
			InputSchema:  extensionsv1.JSON{Raw: []byte(`{"type":"object","required":["message"],"properties":{"message":{"type":"string"}}}`)},
			OutputSchema: extensionsv1.JSON{Raw: []byte(`{"type":"object"}`)},
			Runtime:      taskmolev1alpha1.WorkerRuntimeSpec{Image: "private.invalid/secret-worker:v1", ServiceAccountName: "privileged", TimeoutSeconds: 90}},
		Status: taskmolev1alpha1.WorkerProfileStatus{ObservedGeneration: 1, Conditions: []metav1.Condition{{Type: taskmolev1alpha1.ConditionReady, Status: status, Reason: reason, ObservedGeneration: 1}}}}
}

func testTask(name string, phase taskmolev1alpha1.WorkerTaskPhase) *taskmolev1alpha1.WorkerTask {
	return &taskmolev1alpha1.WorkerTask{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec:   taskmolev1alpha1.WorkerTaskSpec{ProfileRef: testProfileName, Input: extensionsv1.JSON{Raw: []byte(`{"secret":"not-in-list"}`)}},
		Status: taskmolev1alpha1.WorkerTaskStatus{Phase: phase, Result: &extensionsv1.JSON{Raw: []byte(`{"secret":"not-in-list"}`)}}}
}
