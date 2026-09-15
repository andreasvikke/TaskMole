package controller

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	taskmolev1alpha1 "github.com/andreasvikke/taskmole/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	extensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const expectedPendingReason = "Pending"

func TestValidateWorkerResult(t *testing.T) {
	schema := []byte(`{"type":"object","required":["answer"],"properties":{"answer":{"type":"integer"}},"additionalProperties":false}`)
	tests := []struct {
		name, result, reason string
		valid                bool
	}{
		{name: "valid", result: `{"answer":42}`, valid: true},
		{name: "missing", reason: taskmolev1alpha1.ReasonMissingResult},
		{name: "malformed", result: `{"answer":`, reason: taskmolev1alpha1.ReasonInvalidJSON},
		{name: "not object", result: `42`, reason: taskmolev1alpha1.ReasonInvalidJSON},
		{name: "schema mismatch", result: `{"answer":"no"}`, reason: taskmolev1alpha1.ReasonOutputSchemaMismatch},
		{name: "too large", result: `{"answer":42,"padding":"` + strings.Repeat("x", taskmolev1alpha1.MaxResultBytes) + `"}`, reason: taskmolev1alpha1.ReasonResultTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, reason, _ := validateResult(test.result, schema)
			if test.valid && (result == nil || string(result.Raw) != test.result || reason != "") {
				t.Fatalf("result = %#v, reason = %q", result, reason)
			}
			if !test.valid && (result != nil || reason != test.reason) {
				t.Fatalf("result = %#v, reason = %q, want %q", result, reason, test.reason)
			}
		})
	}
}

func TestWorkerTaskLifecycleProjection(t *testing.T) {
	now := metav1.NewTime(time.Unix(1_700_000_000, 0))
	expiredStart := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	tests := []struct {
		name     string
		job      batchv1.JobStatus
		pod      *corev1.Pod
		phase    taskmolev1alpha1.WorkerTaskPhase
		reason   string
		result   string
		exitCode *int32
	}{
		{name: "pending", pod: lifecyclePod(corev1.PodPending, nil), phase: taskmolev1alpha1.WorkerTaskPhasePending, reason: expectedPendingReason},
		{name: "running", job: batchv1.JobStatus{Active: 1, StartTime: &now}, pod: lifecyclePod(corev1.PodRunning, nil), phase: taskmolev1alpha1.WorkerTaskPhaseRunning, reason: "Running"},
		{name: "succeeded", pod: lifecyclePod(corev1.PodSucceeded, terminated(0, `{"answer":42}`, now)), phase: taskmolev1alpha1.WorkerTaskPhaseSucceeded, reason: "Succeeded", result: `{"answer":42}`, exitCode: int32Pointer(0)},
		{name: "nonzero exit", pod: lifecyclePod(corev1.PodFailed, terminated(7, "private raw failure", now)), phase: taskmolev1alpha1.WorkerTaskPhaseFailed, reason: taskmolev1alpha1.ReasonJobFailed, exitCode: int32Pointer(7)},
		{name: "deadline", job: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded", Message: "Job exceeded its deadline"}}}, phase: taskmolev1alpha1.WorkerTaskPhaseTimedOut, reason: taskmolev1alpha1.ReasonDeadlineExceeded},
		{name: "deadline before job condition", job: batchv1.JobStatus{StartTime: &expiredStart}, pod: lifecyclePod(corev1.PodFailed, terminated(137, "", now)), phase: taskmolev1alpha1.WorkerTaskPhaseTimedOut, reason: taskmolev1alpha1.ReasonDeadlineExceeded},
		{name: "evicted", pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "task-test-pod"}, Status: corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted", Message: "low memory"}}, phase: taskmolev1alpha1.WorkerTaskPhaseFailed, reason: taskmolev1alpha1.ReasonJobFailed},
		{name: "completed without pod", job: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}, phase: taskmolev1alpha1.WorkerTaskPhaseFailed, reason: taskmolev1alpha1.ReasonMissingResult},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			scheme := runtime.NewScheme()
			if err := taskmolev1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			task := scheduledTestTask()
			task.CreationTimestamp = now
			task.Spec.ProfileSnapshot.OutputSchema = extensionsv1.JSON{Raw: []byte(`{"type":"object","required":["answer"],"properties":{"answer":{"type":"integer"}}}`)}
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(task).WithObjects(task).Build()
			reconciler := &WorkerTaskReconciler{Client: fakeClient, Scheme: scheme}
			deadline := int64(task.Spec.ProfileSnapshot.Runtime.TimeoutSeconds)
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: task.Name, Namespace: task.Namespace},
				Spec: batchv1.JobSpec{ActiveDeadlineSeconds: &deadline}, Status: test.job}
			if err := reconciler.projectLifecycle(ctx, task, job, test.pod); err != nil {
				t.Fatal(err)
			}
			stored := &taskmolev1alpha1.WorkerTask{}
			if err := fakeClient.Get(ctx, clientKey(task), stored); err != nil {
				t.Fatal(err)
			}
			if stored.Status.Phase != test.phase || stored.Status.Reason != test.reason {
				t.Fatalf("status = %#v", stored.Status)
			}
			if test.result != "" && (stored.Status.Result == nil || string(stored.Status.Result.Raw) != test.result) {
				t.Fatalf("result = %#v", stored.Status.Result)
			}
			if test.exitCode != nil && (stored.Status.ExitCode == nil || *stored.Status.ExitCode != *test.exitCode) {
				t.Fatalf("exit code = %#v", stored.Status.ExitCode)
			}
			if isTerminal(test.phase) && stored.Status.CompletedAt == nil {
				t.Fatal("terminal task has no completion timestamp")
			}
		})
	}
}

func lifecyclePod(phase corev1.PodPhase, state *corev1.ContainerStateTerminated) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "task-test-pod"}, Status: corev1.PodStatus{Phase: phase}}
	if state != nil {
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: workerContainerName, State: corev1.ContainerState{Terminated: state}}}
	}
	return pod
}

func terminated(code int32, message string, finished metav1.Time) *corev1.ContainerStateTerminated {
	return &corev1.ContainerStateTerminated{ExitCode: code, Message: message, FinishedAt: finished}
}

func int32Pointer(value int32) *int32 { return &value }

func clientKey(task *taskmolev1alpha1.WorkerTask) types.NamespacedName {
	return types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
}

func TestWorkerTaskReconcileCreatesSchedulingResourcesOnce(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{taskmolev1alpha1.AddToScheme, corev1.AddToScheme, batchv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	task := scheduledTestTask()
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(task).WithObjects(task).Build()
	reconciler := &WorkerTaskReconciler{Client: client, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: task.Namespace, Name: task.Name}}
	for range 2 {
		if _, err := reconciler.Reconcile(ctx, request); err != nil {
			t.Fatal(err)
		}
	}

	var configMaps corev1.ConfigMapList
	var jobs batchv1.JobList
	if err := client.List(ctx, &configMaps); err != nil || len(configMaps.Items) != 1 {
		t.Fatalf("configmaps = %d, error = %v", len(configMaps.Items), err)
	}
	if err := client.List(ctx, &jobs); err != nil || len(jobs.Items) != 1 {
		t.Fatalf("jobs = %d, error = %v", len(jobs.Items), err)
	}
	configMap := configMaps.Items[0]
	if configMap.Immutable == nil || !*configMap.Immutable || string(configMap.BinaryData[inputKey]) != `{"message":"hello"}` || !metav1.IsControlledBy(&configMap, task) {
		t.Fatalf("unexpected input ConfigMap: %#v", configMap)
	}
	job := jobs.Items[0]
	container := job.Spec.Template.Spec.Containers[0]
	if *job.Spec.BackoffLimit != 0 || *job.Spec.ActiveDeadlineSeconds != 90 || job.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever ||
		container.VolumeMounts[0].MountPath != taskmolev1alpha1.InputPath || container.Env[len(container.Env)-1].Value != taskmolev1alpha1.ResultPath ||
		container.SecurityContext == nil || *container.SecurityContext.AllowPrivilegeEscalation || job.Spec.Template.Spec.SecurityContext == nil ||
		container.SecurityContext.RunAsUser == nil || *container.SecurityContext.RunAsUser != workerUserID ||
		container.SecurityContext.RunAsGroup == nil || *container.SecurityContext.RunAsGroup != workerUserID ||
		!*job.Spec.Template.Spec.SecurityContext.RunAsNonRoot ||
		job.Spec.Template.Spec.SecurityContext.SeccompProfile == nil || job.Spec.Template.Spec.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault ||
		job.Spec.Template.Spec.HostUsers != nil ||
		!metav1.IsControlledBy(&job, task) {
		t.Fatalf("unexpected Job: %#v", job.Spec)
	}
	stored := &taskmolev1alpha1.WorkerTask{}
	if err := client.Get(ctx, request.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.JobName != task.Name || stored.Status.Phase != taskmolev1alpha1.WorkerTaskPhasePending {
		t.Fatalf("unexpected status: %#v", stored.Status)
	}
}

func TestWorkerTaskTerminalStatusIsMonotonic(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{taskmolev1alpha1.AddToScheme, corev1.AddToScheme, batchv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	task := scheduledTestTask()
	zero := int32(0)
	task.Status = taskmolev1alpha1.WorkerTaskStatus{Phase: taskmolev1alpha1.WorkerTaskPhaseSucceeded, JobName: task.Name,
		PodName: task.Name + "-pod", ExitCode: &zero, Reason: "Succeeded", Message: "Worker completed successfully",
		Result: &extensionsv1.JSON{Raw: []byte(`{"answer":42}`)}}
	want := *task.Status.DeepCopy()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(task).WithObjects(task).Build()
	reconciler := &WorkerTaskReconciler{Client: fakeClient, Scheme: scheme}
	request := ctrl.Request{NamespacedName: clientKey(task)}
	for range 3 {
		if _, err := reconciler.Reconcile(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	stored := &taskmolev1alpha1.WorkerTask{}
	if err := fakeClient.Get(ctx, clientKey(task), stored); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stored.Status, want) {
		t.Fatalf("terminal status changed: got %#v, want %#v", stored.Status, want)
	}
}

func TestDesiredJobUsesProfileIdentity(t *testing.T) {
	task := scheduledTestTask()
	task.Spec.ProfileSnapshot.Runtime.RunAsUser = pointer(int64(1000))
	task.Spec.ProfileSnapshot.Runtime.RunAsGroup = pointer(int64(2000))

	securityContext := desiredJob(task).Spec.Template.Spec.Containers[0].SecurityContext
	if securityContext.RunAsUser == nil || *securityContext.RunAsUser != 1000 ||
		securityContext.RunAsGroup == nil || *securityContext.RunAsGroup != 2000 {
		t.Fatalf("unexpected worker identity: %#v", securityContext)
	}
}

func TestDesiredJobUsesRuntimeDefaultSeccompWhenOmitted(t *testing.T) {
	profile := desiredJob(scheduledTestTask()).Spec.Template.Spec.SecurityContext.SeccompProfile
	if profile == nil || profile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatalf("unexpected seccomp profile: %#v", profile)
	}
}

func TestDesiredJobUsesLocalhostSeccompProfile(t *testing.T) {
	task := scheduledTestTask()
	profilePath := "profiles/codex-bwrap.json"
	task.Spec.ProfileSnapshot.Runtime.SeccompProfile = &corev1.SeccompProfile{
		Type: corev1.SeccompProfileTypeLocalhost, LocalhostProfile: &profilePath,
	}

	profile := desiredJob(task).Spec.Template.Spec.SecurityContext.SeccompProfile
	if profile == task.Spec.ProfileSnapshot.Runtime.SeccompProfile {
		t.Fatal("seccomp profile was not deep-copied")
	}
	if profile == nil || profile.Type != corev1.SeccompProfileTypeLocalhost || profile.LocalhostProfile == nil || *profile.LocalhostProfile != profilePath {
		t.Fatalf("unexpected seccomp profile: %#v", profile)
	}
}

func TestDesiredJobUsesUnmaskedProcMount(t *testing.T) {
	task := scheduledTestTask()
	task.Spec.ProfileSnapshot.Runtime.ProcMount = pointer(corev1.UnmaskedProcMount)

	job := desiredJob(task)
	if procMount := job.Spec.Template.Spec.Containers[0].SecurityContext.ProcMount; procMount == nil || *procMount != corev1.UnmaskedProcMount {
		t.Fatalf("unexpected proc mount: %#v", procMount)
	}
	if job.Spec.Template.Spec.HostUsers == nil || *job.Spec.Template.Spec.HostUsers {
		t.Fatalf("hostUsers must be false for an unmasked proc mount: %#v", job.Spec.Template.Spec.HostUsers)
	}
}

func scheduledTestTask() *taskmolev1alpha1.WorkerTask {
	schema := extensionsv1.JSON{Raw: []byte(`{"type":"object"}`)}
	return &taskmolev1alpha1.WorkerTask{ObjectMeta: metav1.ObjectMeta{Name: "task-test", Namespace: "default", UID: "task-uid", Generation: 1},
		Spec: taskmolev1alpha1.WorkerTaskSpec{ProfileRef: workerContainerName, IdempotencyKeyHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Input: extensionsv1.JSON{Raw: []byte(`{"message":"hello"}`)}, ProfileSnapshot: taskmolev1alpha1.WorkerProfileSnapshot{InputSchema: schema, OutputSchema: schema,
				Runtime: taskmolev1alpha1.WorkerRuntimeSpec{Image: "example.invalid/worker:v1", TimeoutSeconds: 90}}}}
}
