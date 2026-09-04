package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	extensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	taskmolev1alpha1 "github.com/andreasvikke/taskmole/api/v1alpha1"
	internalschema "github.com/andreasvikke/taskmole/internal/schema"
)

const (
	maxStatusMessageBytes = 1024
	pendingReason         = "Pending"
)

func isTerminal(phase taskmolev1alpha1.WorkerTaskPhase) bool {
	return phase == taskmolev1alpha1.WorkerTaskPhaseSucceeded || phase == taskmolev1alpha1.WorkerTaskPhaseFailed || phase == taskmolev1alpha1.WorkerTaskPhaseTimedOut
}

func (r *WorkerTaskReconciler) workerPod(ctx context.Context, task *taskmolev1alpha1.WorkerTask, job *batchv1.Job) (*corev1.Pod, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(task.Namespace), client.MatchingLabels{taskNameLabel: task.Name}); err != nil {
		return nil, err
	}
	matching := make([]*corev1.Pod, 0, len(pods.Items))
	for index := range pods.Items {
		pod := &pods.Items[index]
		if metav1.IsControlledBy(pod, job) || job.UID == "" {
			matching = append(matching, pod)
		}
	}
	if len(matching) == 0 {
		return nil, nil
	}
	slices.SortFunc(matching, func(left, right *corev1.Pod) int {
		return left.CreationTimestamp.Compare(right.CreationTimestamp.Time)
	})
	return matching[len(matching)-1], nil
}

func (r *WorkerTaskReconciler) projectLifecycle(ctx context.Context, task *taskmolev1alpha1.WorkerTask, job *batchv1.Job, pod *corev1.Pod) error {
	base := task.DeepCopy()
	status := &task.Status
	status.ObservedGeneration = task.Generation
	status.JobName = job.Name
	if status.CreatedAt == nil && !task.CreationTimestamp.IsZero() {
		status.CreatedAt = task.CreationTimestamp.DeepCopy()
	}
	setTaskCondition(status, taskmolev1alpha1.ConditionScheduled, metav1.ConditionTrue, taskmolev1alpha1.ReasonScheduled, "Job was scheduled", task.Generation)

	if job.Status.StartTime != nil && status.StartedAt == nil {
		status.StartedAt = job.Status.StartTime.DeepCopy()
	}
	if pod != nil {
		status.PodName = pod.Name
		if status.StartedAt == nil && pod.Status.StartTime != nil {
			status.StartedAt = pod.Status.StartTime.DeepCopy()
		}
	}

	if failed := findJobCondition(job.Status.Conditions, batchv1.JobFailed); failed != nil {
		if failed.Reason == "DeadlineExceeded" || failed.Reason == "DeadlineExceededFailureTarget" ||
			activeDeadlineExceeded(job, status.StartedAt, time.Now()) {
			completeTask(status, taskmolev1alpha1.WorkerTaskPhaseTimedOut, taskmolev1alpha1.ReasonDeadlineExceeded, failed.Message, completionTime(job, pod), task.Generation)
		} else {
			code, message := failureDetails(pod, failed.Reason, failed.Message)
			status.ExitCode = code
			completeTask(status, taskmolev1alpha1.WorkerTaskPhaseFailed, taskmolev1alpha1.ReasonJobFailed, message, completionTime(job, pod), task.Generation)
		}
	} else if terminated := workerTermination(pod); terminated != nil {
		status.ExitCode = &terminated.ExitCode
		if terminated.ExitCode != 0 {
			if activeDeadlineExceeded(job, status.StartedAt, time.Now()) {
				completeTask(status, taskmolev1alpha1.WorkerTaskPhaseTimedOut, taskmolev1alpha1.ReasonDeadlineExceeded,
					"Job exceeded its active deadline", completionTime(job, pod), task.Generation)
			} else {
				completeTask(status, taskmolev1alpha1.WorkerTaskPhaseFailed, taskmolev1alpha1.ReasonJobFailed,
					terminationFailureMessage(terminated), completionTime(job, pod), task.Generation)
			}
		} else {
			result, reason, message := validateResult(terminated.Message, task.Spec.ProfileSnapshot.OutputSchema.Raw)
			if reason != "" {
				completeTask(status, taskmolev1alpha1.WorkerTaskPhaseFailed, reason, message, completionTime(job, pod), task.Generation)
			} else {
				status.Result = result
				completeTask(status, taskmolev1alpha1.WorkerTaskPhaseSucceeded, "Succeeded", "Worker completed successfully", completionTime(job, pod), task.Generation)
			}
		}
	} else if findJobCondition(job.Status.Conditions, batchv1.JobComplete) != nil {
		completeTask(status, taskmolev1alpha1.WorkerTaskPhaseFailed, taskmolev1alpha1.ReasonMissingResult, "Completed Job has no readable worker result", completionTime(job, pod), task.Generation)
	} else if pod != nil && pod.Status.Phase == corev1.PodFailed {
		if activeDeadlineExceeded(job, status.StartedAt, time.Now()) {
			completeTask(status, taskmolev1alpha1.WorkerTaskPhaseTimedOut, taskmolev1alpha1.ReasonDeadlineExceeded,
				"Job exceeded its active deadline", completionTime(job, pod), task.Generation)
		} else {
			code, message := failureDetails(pod, pod.Status.Reason, pod.Status.Message)
			status.ExitCode = code
			completeTask(status, taskmolev1alpha1.WorkerTaskPhaseFailed, taskmolev1alpha1.ReasonJobFailed,
				message, completionTime(job, pod), task.Generation)
		}
	} else if status.StartedAt != nil || job.Status.Active > 0 || (pod != nil && pod.Status.Phase == corev1.PodRunning) {
		status.Phase = taskmolev1alpha1.WorkerTaskPhaseRunning
		status.Reason = "Running"
		status.Message = "Worker is running"
		setTaskCondition(status, taskmolev1alpha1.ConditionCompleted, metav1.ConditionFalse, "Running", "Worker is running", task.Generation)
	} else {
		status.Phase = taskmolev1alpha1.WorkerTaskPhasePending
		status.Reason, status.Message = pendingDetails(pod)
		setTaskCondition(status, taskmolev1alpha1.ConditionCompleted, metav1.ConditionFalse, status.Reason, status.Message, task.Generation)
	}
	return r.Status().Patch(ctx, task, client.MergeFrom(base))
}

func activeDeadlineExceeded(job *batchv1.Job, fallbackStart *metav1.Time, now time.Time) bool {
	if job.Spec.ActiveDeadlineSeconds == nil {
		return false
	}
	start := job.Status.StartTime
	if start == nil {
		start = fallbackStart
	}
	if start == nil {
		return false
	}
	deadline := start.Add(time.Duration(*job.Spec.ActiveDeadlineSeconds) * time.Second)
	return !now.Before(deadline)
}

func validateResult(message string, rawSchema []byte) (*extensionsv1.JSON, string, string) {
	if message == "" {
		return nil, taskmolev1alpha1.ReasonMissingResult, "Worker did not write a result"
	}
	if len([]byte(message)) > taskmolev1alpha1.MaxResultBytes {
		return nil, taskmolev1alpha1.ReasonResultTooLarge, fmt.Sprintf("Worker result exceeds the %d byte limit", taskmolev1alpha1.MaxResultBytes)
	}
	document, err := internalschema.Decode([]byte(message))
	if err != nil {
		return nil, taskmolev1alpha1.ReasonInvalidJSON, "Worker result is not valid JSON"
	}
	if _, ok := document.(map[string]any); !ok {
		return nil, taskmolev1alpha1.ReasonInvalidJSON, "Worker result must be a JSON object"
	}
	if err := internalschema.Validate(rawSchema, "urn:taskmole:task-output", document); err != nil {
		return nil, taskmolev1alpha1.ReasonOutputSchemaMismatch, "Worker result does not match the snapshotted output schema"
	}
	return &extensionsv1.JSON{Raw: []byte(message)}, "", ""
}

func completeTask(status *taskmolev1alpha1.WorkerTaskStatus, phase taskmolev1alpha1.WorkerTaskPhase, reason, message string, completedAt *metav1.Time, generation int64) {
	status.Phase, status.Reason, status.Message = phase, reason, boundedMessage(message)
	status.CompletedAt = completedAt
	conditionStatus := metav1.ConditionFalse
	if phase == taskmolev1alpha1.WorkerTaskPhaseSucceeded {
		conditionStatus = metav1.ConditionTrue
	}
	setTaskCondition(status, taskmolev1alpha1.ConditionCompleted, conditionStatus, reason, status.Message, generation)
}

func setTaskCondition(status *taskmolev1alpha1.WorkerTaskStatus, conditionType string, conditionStatus metav1.ConditionStatus, reason, message string, generation int64) {
	apiMeta.SetStatusCondition(&status.Conditions, metav1.Condition{Type: conditionType, Status: conditionStatus, Reason: reason, Message: boundedMessage(message), ObservedGeneration: generation})
}

func pendingDetails(pod *corev1.Pod) (string, string) {
	if pod == nil {
		return pendingReason, "Waiting for a worker Pod"
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Status == corev1.ConditionFalse && condition.Message != "" {
			return defaultReason(condition.Reason, pendingReason), boundedMessage(condition.Message)
		}
	}
	for _, container := range pod.Status.ContainerStatuses {
		if container.State.Waiting != nil {
			return defaultReason(container.State.Waiting.Reason, pendingReason), boundedMessage(container.State.Waiting.Message)
		}
	}
	return pendingReason, "Worker is pending"
}

func failureDetails(pod *corev1.Pod, fallbackReason, fallbackMessage string) (*int32, string) {
	if terminated := workerTermination(pod); terminated != nil {
		return &terminated.ExitCode, terminationFailureMessage(terminated)
	}
	if pod != nil && (pod.Status.Reason != "" || pod.Status.Message != "") {
		return nil, strings.TrimSpace(pod.Status.Reason + ": " + pod.Status.Message)
	}
	return nil, strings.TrimSpace(defaultReason(fallbackReason, "Job failed") + ": " + fallbackMessage)
}

func workerTermination(pod *corev1.Pod) *corev1.ContainerStateTerminated {
	if pod == nil {
		return nil
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == workerContainerName {
			return status.State.Terminated
		}
	}
	return nil
}

func terminationFailureMessage(terminated *corev1.ContainerStateTerminated) string {
	return fmt.Sprintf("Worker exited with code %d (%s)", terminated.ExitCode, defaultReason(terminated.Reason, "Error"))
}

func findJobCondition(conditions []batchv1.JobCondition, conditionType batchv1.JobConditionType) *batchv1.JobCondition {
	for index := range conditions {
		if conditions[index].Type == conditionType && conditions[index].Status == corev1.ConditionTrue {
			return &conditions[index]
		}
	}
	return nil
}

func completionTime(job *batchv1.Job, pod *corev1.Pod) *metav1.Time {
	if job.Status.CompletionTime != nil {
		return job.Status.CompletionTime.DeepCopy()
	}
	if terminated := workerTermination(pod); terminated != nil && !terminated.FinishedAt.IsZero() {
		return terminated.FinishedAt.DeepCopy()
	}
	now := metav1.Now()
	return &now
}

func defaultReason(reason, fallback string) string {
	if reason != "" {
		return reason
	}
	return fallback
}

func boundedMessage(message string) string {
	message = strings.TrimSpace(message)
	if len(message) <= maxStatusMessageBytes {
		return message
	}
	for len(message) > maxStatusMessageBytes {
		_, size := utf8.DecodeLastRuneInString(message)
		message = message[:len(message)-size]
	}
	return message
}
