package v1alpha1

import "time"

const (
	MaxInputBytes  = 256 * 1024
	MaxResultBytes = 4 * 1024
	// MaxSchemaBytes and MaxSchemaDepth bound profile validation and discovery work.
	MaxSchemaBytes = 64 * 1024
	MaxSchemaDepth = 32
	MinTaskTimeout = 60 * time.Second
	MaxTaskTimeout = 24 * time.Hour
	InputPath      = "/taskmole/input.json"
	ResultPath     = "/dev/termination-log"
)

// WorkerTaskPhase is a canonical task lifecycle state.
type WorkerTaskPhase string

const (
	WorkerTaskPhasePending   WorkerTaskPhase = "Pending"
	WorkerTaskPhaseRunning   WorkerTaskPhase = "Running"
	WorkerTaskPhaseSucceeded WorkerTaskPhase = "Succeeded"
	WorkerTaskPhaseFailed    WorkerTaskPhase = "Failed"
	WorkerTaskPhaseTimedOut  WorkerTaskPhase = "TimedOut"
)

const (
	ConditionReady     = "Ready"
	ConditionScheduled = "Scheduled"
	ConditionCompleted = "Completed"
)

const (
	ReasonValid                       = "Valid"
	ReasonInvalidSchema               = "InvalidSchema"
	ReasonInvalidRuntimeConfiguration = "InvalidRuntimeConfiguration"
	ReasonScheduled                   = "Scheduled"
	ReasonJobFailed                   = "JobFailed"
	ReasonDeadlineExceeded            = "DeadlineExceeded"
	ReasonMissingResult               = "MissingResult"
	ReasonInvalidJSON                 = "InvalidJSON"
	ReasonOutputSchemaMismatch        = "OutputSchemaMismatch"
	ReasonResultTooLarge              = "ResultTooLarge"
)
