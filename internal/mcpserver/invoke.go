package mcpserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	extensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	taskmolev1alpha1 "github.com/andreasvikke/taskmole/api/v1alpha1"
	internalschema "github.com/andreasvikke/taskmole/internal/schema"
)

var (
	errInvalidInput        = errors.New("invalid invocation input")
	errInputTooLarge       = errors.New("invocation input is too large")
	errProfileNotFound     = errors.New("worker profile was not found")
	errProfileUnavailable  = errors.New("worker profile is unavailable")
	errIdempotencyConflict = errors.New("idempotency key conflicts with an existing task")
)

type invokeWorkerInput struct {
	ProfileName    string `json:"profileName" jsonschema:"name of the discovered worker profile"`
	Input          any    `json:"input" jsonschema:"input matching the worker profile input schema"`
	IdempotencyKey string `json:"idempotencyKey" jsonschema:"required caller-generated key used to safely retry invocation"`
}

type invokeWorkerOutput struct {
	TaskID string `json:"taskId"`
}

func (s *Server) invokeWorker(ctx context.Context, _ *mcp.CallToolRequest, input invokeWorkerInput) (*mcp.CallToolResult, invokeWorkerOutput, error) {
	raw, err := json.Marshal(input.Input)
	if err != nil {
		return nil, invokeWorkerOutput{}, toolError("invalid_input", "invocation input is not valid JSON")
	}
	task, err := s.invoke(ctx, input.ProfileName, input.IdempotencyKey, raw)
	if err != nil {
		return nil, invokeWorkerOutput{}, invocationError(err)
	}
	return nil, invokeWorkerOutput{TaskID: task.Name}, nil
}

func (s *Server) invoke(ctx context.Context, profileName, idempotencyKey string, rawInput []byte) (*taskmolev1alpha1.WorkerTask, error) {
	if profileName == "" || idempotencyKey == "" {
		return nil, errInvalidInput
	}
	if len(rawInput) > taskmolev1alpha1.MaxInputBytes {
		return nil, errInputTooLarge
	}
	input, err := internalschema.Decode(rawInput)
	if err != nil {
		return nil, errInvalidInput
	}

	keyHash := sha256.Sum256([]byte(idempotencyKey))
	hash := hex.EncodeToString(keyHash[:])
	name := "task-" + hash[:58]
	existing := &taskmolev1alpha1.WorkerTask{}
	err = s.Client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: name}, existing)
	if err == nil {
		if existing.Spec.IdempotencyKeyHash != hash || existing.Spec.ProfileRef != profileName || !jsonEqual(existing.Spec.Input.Raw, rawInput) {
			return nil, errIdempotencyConflict
		}
		return existing, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get existing task: %w", err)
	}

	profile := &taskmolev1alpha1.WorkerProfile{}
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: profileName}, profile); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, errProfileNotFound
		}
		return nil, fmt.Errorf("get worker profile: %w", err)
	}
	ready := apiMeta.FindStatusCondition(profile.Status.Conditions, taskmolev1alpha1.ConditionReady)
	if ready == nil || ready.Status != metav1.ConditionTrue || ready.ObservedGeneration != profile.Generation {
		return nil, errProfileUnavailable
	}
	if err := internalschema.Validate(profile.Spec.InputSchema.Raw, "urn:taskmole:invocation-input", input); err != nil {
		return nil, errInvalidInput
	}

	task := &taskmolev1alpha1.WorkerTask{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.Namespace}, Spec: taskmolev1alpha1.WorkerTaskSpec{
		ProfileRef: profileName, IdempotencyKeyHash: hash, Input: extensionsv1.JSON{Raw: append([]byte(nil), rawInput...)},
		ProfileSnapshot: taskmolev1alpha1.WorkerProfileSnapshot{
			InputSchema: extensionsv1.JSON{Raw: append([]byte(nil), profile.Spec.InputSchema.Raw...)}, OutputSchema: extensionsv1.JSON{Raw: append([]byte(nil), profile.Spec.OutputSchema.Raw...)}, Runtime: *profile.Spec.Runtime.DeepCopy(),
		},
	}}
	if err := s.Client.Create(ctx, task); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return s.invoke(ctx, profileName, idempotencyKey, rawInput)
		}
		return nil, fmt.Errorf("create worker task: %w", err)
	}
	return task, nil
}

func jsonEqual(left, right []byte) bool {
	var a, b any
	return json.Unmarshal(left, &a) == nil && json.Unmarshal(right, &b) == nil && reflect.DeepEqual(a, b)
}

func invocationError(err error) error {
	switch {
	case errors.Is(err, errProfileNotFound):
		return toolError("profile_not_found", "worker profile was not found")
	case errors.Is(err, errProfileUnavailable):
		return toolError("profile_unavailable", "worker profile is not Ready")
	case errors.Is(err, errInputTooLarge):
		return toolError("input_too_large", fmt.Sprintf("input exceeds %d bytes", taskmolev1alpha1.MaxInputBytes))
	case errors.Is(err, errIdempotencyConflict):
		return toolError("idempotency_conflict", "idempotency key is already associated with different invocation data")
	case errors.Is(err, errInvalidInput):
		return toolError("invalid_input", "invocation input is invalid or does not match the worker schema")
	default:
		return toolError("kubernetes_failure", "could not create worker task")
	}
}
