// Package mcpserver exposes TaskMole's transport-neutral services over MCP.
package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	taskmolev1alpha1 "github.com/andreasvikke/taskmole/api/v1alpha1"
)

const defaultPageSize int64 = 50

// Server is an MCP server and a controller-runtime Runnable.
type Server struct {
	Address   string
	Namespace string
	Client    client.Client
	HTTP      *http.Server
}

// +kubebuilder:rbac:groups=taskmole.io,namespace=taskmole,resources=workerprofiles,verbs=get;list
// +kubebuilder:rbac:groups=taskmole.io,namespace=taskmole,resources=workertasks,verbs=get;list;create

type listWorkersInput struct {
	IncludeUnavailable bool   `json:"includeUnavailable,omitempty" jsonschema:"include profiles that are not currently Ready"`
	Limit              int64  `json:"limit,omitempty" jsonschema:"maximum Kubernetes objects to inspect, from 1 to 100"`
	Continue           string `json:"continue,omitempty" jsonschema:"opaque continuation token returned by the previous call"`
}

type worker struct {
	Name              string         `json:"name"`
	DisplayName       string         `json:"displayName"`
	Description       string         `json:"description"`
	Tags              []string       `json:"tags,omitempty"`
	InputSchema       map[string]any `json:"inputSchema"`
	OutputSchema      map[string]any `json:"outputSchema"`
	Ready             bool           `json:"ready"`
	UnavailableReason string         `json:"unavailableReason,omitempty"`
}

type listWorkersOutput struct {
	Workers  []worker `json:"workers"`
	Continue string   `json:"continue,omitempty"`
}

type getTaskInput struct {
	TaskID string `json:"taskId"`
}

type taskSummary struct {
	TaskID      string                           `json:"taskId"`
	ProfileName string                           `json:"profileName"`
	Phase       taskmolev1alpha1.WorkerTaskPhase `json:"phase"`
	CreatedAt   string                           `json:"createdAt,omitempty"`
	StartedAt   string                           `json:"startedAt,omitempty"`
	CompletedAt string                           `json:"completedAt,omitempty"`
}

type taskDetail struct {
	TaskID      string                           `json:"taskId"`
	ProfileName string                           `json:"profileName"`
	Phase       taskmolev1alpha1.WorkerTaskPhase `json:"phase"`
	CreatedAt   string                           `json:"createdAt,omitempty"`
	StartedAt   string                           `json:"startedAt,omitempty"`
	CompletedAt string                           `json:"completedAt,omitempty"`
	Input       any                              `json:"input"`
	JobName     string                           `json:"jobName,omitempty"`
	PodName     string                           `json:"podName,omitempty"`
	ExitCode    *int32                           `json:"exitCode,omitempty"`
	Reason      string                           `json:"reason,omitempty"`
	Message     string                           `json:"message,omitempty"`
	Result      any                              `json:"result,omitempty"`
	Conditions  []condition                      `json:"conditions,omitempty"`
}

type condition struct {
	Type               string                 `json:"type"`
	Status             metav1.ConditionStatus `json:"status"`
	ObservedGeneration int64                  `json:"observedGeneration,omitempty"`
	LastTransitionTime string                 `json:"lastTransitionTime"`
	Reason             string                 `json:"reason"`
	Message            string                 `json:"message,omitempty"`
}

type listTasksInput struct {
	Limit    int64  `json:"limit,omitempty" jsonschema:"maximum Kubernetes objects to inspect, from 1 to 100"`
	Continue string `json:"continue,omitempty" jsonschema:"opaque continuation token returned by the previous call"`
}

type listTasksOutput struct {
	Tasks    []taskSummary `json:"tasks"`
	Continue string        `json:"continue,omitempty"`
}

// New constructs the MCP interface with exactly the five public v1 tools.
func New(address, namespace string, kubeClient client.Client) *Server {
	s := &Server{Address: address, Namespace: namespace, Client: kubeClient}
	mcpServer := mcp.NewServer(&mcp.Implementation{Name: "taskmole", Version: "v1alpha1"},
		&mcp.ServerOptions{Capabilities: &mcp.ServerCapabilities{}})
	mcp.AddTool(mcpServer, &mcp.Tool{Name: "list_workers", Description: "List discoverable worker profiles and their input/output schemas"}, s.listWorkers)
	mcp.AddTool(mcpServer, &mcp.Tool{Name: "invoke_worker", Description: "Durably create a worker task and immediately return its stable ID"}, s.invokeWorker)
	mcp.AddTool(mcpServer, &mcp.Tool{Name: "get_worker_task", Description: "Get complete durable state for one worker task"}, s.getWorkerTask)
	mcp.AddTool(mcpServer, &mcp.Tool{Name: "list_worker_tasks", Description: "List compact worker task summaries"}, s.listWorkerTasks)
	mcp.AddTool(mcpServer, &mcp.Tool{Name: "list_running_worker_tasks", Description: "List compact summaries for Pending and Running worker tasks"}, s.listRunningWorkerTasks)
	mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return mcpServer }, &mcp.StreamableHTTPOptions{
		Stateless:    true,
		JSONResponse: true,
	})
	s.HTTP = &http.Server{
		Addr: address,
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.URL.Path != "/" && request.URL.Path != "/mcp" {
				http.NotFound(writer, request)
				return
			}
			logger := log.FromContext(request.Context()).WithValues("method", request.Method, "path", request.URL.Path)
			logger.Info("Handling MCP request")
			mcpHandler.ServeHTTP(writer, request)
			logger.Info("Handled MCP request")
		}),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s
}

// Start serves MCP independently from the manager health server.
func (s *Server) Start(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.HTTP.Shutdown(shutdownCtx)
	}()
	err := s.HTTP.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) listWorkers(ctx context.Context, _ *mcp.CallToolRequest, input listWorkersInput) (*mcp.CallToolResult, listWorkersOutput, error) {
	limit, err := pageLimit(input.Limit)
	if err != nil {
		return nil, listWorkersOutput{}, err
	}
	var profiles taskmolev1alpha1.WorkerProfileList
	if err := s.Client.List(ctx, &profiles, client.InNamespace(s.Namespace), client.Limit(limit), client.Continue(input.Continue)); err != nil {
		return nil, listWorkersOutput{}, toolError("kubernetes_failure", "could not list worker profiles")
	}
	out := listWorkersOutput{Workers: []worker{}, Continue: profiles.Continue}
	for i := range profiles.Items {
		profile := &profiles.Items[i]
		condition := apiMeta.FindStatusCondition(profile.Status.Conditions, taskmolev1alpha1.ConditionReady)
		ready := condition != nil && condition.Status == metav1.ConditionTrue && condition.ObservedGeneration == profile.Generation
		if !ready && !input.IncludeUnavailable {
			continue
		}
		inputSchema, inputErr := decodeJSONObject(profile.Spec.InputSchema.Raw)
		outputSchema, outputErr := decodeJSONObject(profile.Spec.OutputSchema.Raw)
		if inputErr != nil || outputErr != nil {
			return nil, listWorkersOutput{}, toolError("kubernetes_failure", "worker profile contains an unreadable schema")
		}
		item := worker{Name: profile.Name, DisplayName: profile.Spec.DisplayName, Description: profile.Spec.Description,
			Tags: append([]string(nil), profile.Spec.Tags...), InputSchema: inputSchema,
			OutputSchema: outputSchema, Ready: ready}
		if !ready {
			item.UnavailableReason = safeUnavailableReason(condition)
		}
		out.Workers = append(out.Workers, item)
	}
	return nil, out, nil
}

func (s *Server) getWorkerTask(ctx context.Context, _ *mcp.CallToolRequest, input getTaskInput) (*mcp.CallToolResult, taskDetail, error) {
	var task taskmolev1alpha1.WorkerTask
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: input.TaskID}, &task); err != nil {
		if client.IgnoreNotFound(err) == nil {
			return nil, taskDetail{}, toolError("task_not_found", "worker task was not found")
		}
		return nil, taskDetail{}, toolError("kubernetes_failure", "could not get worker task")
	}
	summary := summarize(&task)
	taskInput, err := decodeJSON(task.Spec.Input.Raw)
	if err != nil {
		return nil, taskDetail{}, toolError("kubernetes_failure", "worker task contains unreadable input")
	}
	detail := taskDetail{TaskID: summary.TaskID, ProfileName: summary.ProfileName, Phase: summary.Phase,
		CreatedAt: summary.CreatedAt, StartedAt: summary.StartedAt, CompletedAt: summary.CompletedAt,
		Input: taskInput, JobName: task.Status.JobName,
		PodName: task.Status.PodName, ExitCode: task.Status.ExitCode, Reason: task.Status.Reason, Message: task.Status.Message,
		Conditions: conditions(task.Status.Conditions)}
	if task.Status.Result != nil {
		detail.Result, err = decodeJSON(task.Status.Result.Raw)
		if err != nil {
			return nil, taskDetail{}, toolError("kubernetes_failure", "worker task contains unreadable result")
		}
	}
	return nil, detail, nil
}

func (s *Server) listWorkerTasks(ctx context.Context, _ *mcp.CallToolRequest, input listTasksInput) (*mcp.CallToolResult, listTasksOutput, error) {
	return s.listTasks(ctx, input, false)
}

func (s *Server) listRunningWorkerTasks(ctx context.Context, _ *mcp.CallToolRequest, input listTasksInput) (*mcp.CallToolResult, listTasksOutput, error) {
	return s.listTasks(ctx, input, true)
}

func (s *Server) listTasks(ctx context.Context, input listTasksInput, runningOnly bool) (*mcp.CallToolResult, listTasksOutput, error) {
	limit, err := pageLimit(input.Limit)
	if err != nil {
		return nil, listTasksOutput{}, err
	}
	var tasks taskmolev1alpha1.WorkerTaskList
	if err := s.Client.List(ctx, &tasks, client.InNamespace(s.Namespace), client.Limit(limit), client.Continue(input.Continue)); err != nil {
		return nil, listTasksOutput{}, toolError("kubernetes_failure", "could not list worker tasks")
	}
	out := listTasksOutput{Tasks: []taskSummary{}, Continue: tasks.Continue}
	for i := range tasks.Items {
		if runningOnly && tasks.Items[i].Status.Phase != taskmolev1alpha1.WorkerTaskPhasePending && tasks.Items[i].Status.Phase != taskmolev1alpha1.WorkerTaskPhaseRunning {
			continue
		}
		out.Tasks = append(out.Tasks, summarize(&tasks.Items[i]))
	}
	return nil, out, nil
}

func summarize(task *taskmolev1alpha1.WorkerTask) taskSummary {
	return taskSummary{TaskID: task.Name, ProfileName: task.Spec.ProfileRef, Phase: task.Status.Phase,
		CreatedAt: timestamp(task.Status.CreatedAt), StartedAt: timestamp(task.Status.StartedAt), CompletedAt: timestamp(task.Status.CompletedAt)}
}

func timestamp(value *metav1.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func conditions(values []metav1.Condition) []condition {
	out := make([]condition, 0, len(values))
	for _, value := range values {
		out = append(out, condition{Type: value.Type, Status: value.Status, ObservedGeneration: value.ObservedGeneration,
			LastTransitionTime: value.LastTransitionTime.UTC().Format(time.RFC3339Nano), Reason: value.Reason, Message: value.Message})
	}
	return out
}

func pageLimit(limit int64) (int64, error) {
	if limit == 0 {
		return defaultPageSize, nil
	}
	if limit < 1 || limit > 100 {
		return 0, toolError("invalid_request", "limit must be between 1 and 100")
	}
	return limit, nil
}

func safeUnavailableReason(condition *metav1.Condition) string {
	if condition == nil {
		return "Ready condition has not been reported"
	}
	if condition.Reason == "" {
		return "Worker profile is not Ready"
	}
	return condition.Reason
}

func decodeJSON(raw []byte) (any, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeJSONObject(raw []byte) (map[string]any, error) {
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func toolError(code, message string) error { return fmt.Errorf("%s: %s", code, message) }

var _ interface{ Start(context.Context) error } = (*Server)(nil)
