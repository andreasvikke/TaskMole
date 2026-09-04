package v1alpha1

import (
	"encoding/json"
	"reflect"
	"testing"

	extensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

func TestRawJSONRoundTrip(t *testing.T) {
	raw := []byte(`{"text":"unchanged","nested":{"number":1.25,"boolean":true},"array":[null,"value"]}`)
	original := WorkerTaskSpec{Input: extensionsv1.JSON{Raw: raw}}

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal task spec: %v", err)
	}

	var decoded WorkerTaskSpec
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal task spec: %v", err)
	}
	if string(decoded.Input.Raw) != string(raw) {
		t.Fatalf("raw JSON changed: got %s, want %s", decoded.Input.Raw, raw)
	}
}

func TestContractConstants(t *testing.T) {
	got := []any{
		WorkerTaskPhasePending, WorkerTaskPhaseRunning, WorkerTaskPhaseSucceeded,
		WorkerTaskPhaseFailed, WorkerTaskPhaseTimedOut,
		ConditionReady, ConditionScheduled, ConditionCompleted,
		ReasonValid, ReasonInvalidSchema, ReasonInvalidRuntimeConfiguration,
		ReasonScheduled, ReasonJobFailed, ReasonDeadlineExceeded,
		ReasonMissingResult, ReasonInvalidJSON, ReasonOutputSchemaMismatch, ReasonResultTooLarge,
		MaxInputBytes, MaxResultBytes, MaxSchemaBytes, MaxSchemaDepth, InputPath, ResultPath,
	}
	want := []any{
		WorkerTaskPhase("Pending"), WorkerTaskPhase("Running"), WorkerTaskPhase("Succeeded"),
		WorkerTaskPhase("Failed"), WorkerTaskPhase("TimedOut"),
		"Ready", "Scheduled", "Completed",
		"Valid", "InvalidSchema", "InvalidRuntimeConfiguration",
		"Scheduled", "JobFailed", "DeadlineExceeded",
		"MissingResult", "InvalidJSON", "OutputSchemaMismatch", "ResultTooLarge",
		256 * 1024, 4 * 1024, 64 * 1024, 32, "/taskmole/input.json", "/dev/termination-log",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("public contract changed:\n got: %#v\nwant: %#v", got, want)
	}
}
