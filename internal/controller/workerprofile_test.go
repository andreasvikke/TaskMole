package controller

import (
	"context"
	"strings"
	"testing"

	taskmolev1alpha1 "github.com/andreasvikke/taskmole/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	extensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestValidateProfile(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*taskmolev1alpha1.WorkerProfile)
		reason string
	}{
		{name: "valid nested local reference", mutate: func(profile *taskmolev1alpha1.WorkerProfile) {
			profile.Spec.InputSchema.Raw = []byte(`{"$schema":"https://json-schema.org/draft/2020-12/schema","$defs":{"name":{"type":"string"}},"properties":{"name":{"$ref":"#/$defs/name"}}}`)
		}, reason: taskmolev1alpha1.ReasonValid},
		{name: "remote reference", mutate: func(profile *taskmolev1alpha1.WorkerProfile) {
			profile.Spec.InputSchema.Raw = []byte(`{"$ref":"https://example.com/schema.json"}`)
		}, reason: taskmolev1alpha1.ReasonInvalidSchema},
		{name: "invalid schema keyword", mutate: func(profile *taskmolev1alpha1.WorkerProfile) {
			profile.Spec.OutputSchema.Raw = []byte(`{"type":12}`)
		}, reason: taskmolev1alpha1.ReasonInvalidSchema},
		{name: "oversized schema", mutate: func(profile *taskmolev1alpha1.WorkerProfile) {
			profile.Spec.InputSchema.Raw = []byte(`{"description":"` + strings.Repeat("x", taskmolev1alpha1.MaxSchemaBytes) + `"}`)
		}, reason: taskmolev1alpha1.ReasonInvalidSchema},
		{name: "invalid image", mutate: func(profile *taskmolev1alpha1.WorkerProfile) {
			profile.Spec.Runtime.Image = " worker:v1"
		}, reason: taskmolev1alpha1.ReasonInvalidRuntimeConfiguration},
		{name: "valid localhost seccomp profile", mutate: func(profile *taskmolev1alpha1.WorkerProfile) {
			profilePath := "profiles/codex-bwrap.json"
			profile.Spec.Runtime.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeLocalhost, LocalhostProfile: &profilePath}
		}, reason: taskmolev1alpha1.ReasonValid},
		{name: "localhost profile missing path", mutate: func(profile *taskmolev1alpha1.WorkerProfile) {
			profile.Spec.Runtime.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeLocalhost}
		}, reason: taskmolev1alpha1.ReasonInvalidRuntimeConfiguration},
		{name: "localhost profile absolute path", mutate: func(profile *taskmolev1alpha1.WorkerProfile) {
			profilePath := "/profiles/codex-bwrap.json"
			profile.Spec.Runtime.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeLocalhost, LocalhostProfile: &profilePath}
		}, reason: taskmolev1alpha1.ReasonInvalidRuntimeConfiguration},
		{name: "localhost profile traversal", mutate: func(profile *taskmolev1alpha1.WorkerProfile) {
			profilePath := "profiles/../codex-bwrap.json"
			profile.Spec.Runtime.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeLocalhost, LocalhostProfile: &profilePath}
		}, reason: taskmolev1alpha1.ReasonInvalidRuntimeConfiguration},
		{name: "unconfined seccomp profile", mutate: func(profile *taskmolev1alpha1.WorkerProfile) {
			profile.Spec.Runtime.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeUnconfined}
		}, reason: taskmolev1alpha1.ReasonInvalidRuntimeConfiguration},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			profile := validWorkerProfile()
			tc.mutate(profile)
			reason, err := validateProfile(profile)
			if reason != tc.reason {
				t.Fatalf("reason = %q, want %q (error: %v)", reason, tc.reason, err)
			}
			if tc.reason == taskmolev1alpha1.ReasonValid && err != nil {
				t.Fatalf("valid profile returned error: %v", err)
			}
			if tc.reason != taskmolev1alpha1.ReasonValid && err == nil {
				t.Fatal("invalid profile returned no error")
			}
		})
	}
}

func TestWorkerProfileReconcileRevalidatesUpdates(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := taskmolev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	profile := validWorkerProfile()
	profile.Namespace = "default"
	profile.Generation = 1
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(profile).WithObjects(profile).Build()
	reconciler := &WorkerProfileReconciler{Client: client}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: profile.Name, Namespace: profile.Namespace}}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	stored := &taskmolev1alpha1.WorkerProfile{}
	if err := client.Get(context.Background(), request.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	assertReadyCondition(t, stored, metav1.ConditionTrue, taskmolev1alpha1.ReasonValid, 1)

	stored.Spec.Runtime.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeUnconfined}
	stored.Generation = 2
	if err := client.Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), request.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	assertReadyCondition(t, stored, metav1.ConditionFalse, taskmolev1alpha1.ReasonInvalidRuntimeConfiguration, 2)
}

func assertReadyCondition(t *testing.T, profile *taskmolev1alpha1.WorkerProfile, status metav1.ConditionStatus, reason string, generation int64) {
	t.Helper()
	if profile.Status.ObservedGeneration != generation || len(profile.Status.Conditions) != 1 {
		t.Fatalf("unexpected status: %#v", profile.Status)
	}
	condition := profile.Status.Conditions[0]
	if condition.Status != status || condition.Reason != reason || condition.ObservedGeneration != generation {
		t.Fatalf("unexpected Ready condition: %#v", condition)
	}
}

func validWorkerProfile() *taskmolev1alpha1.WorkerProfile {
	schema := extensionsv1.JSON{Raw: []byte(`{"type":"object"}`)}
	return &taskmolev1alpha1.WorkerProfile{
		ObjectMeta: metav1.ObjectMeta{Name: workerContainerName},
		Spec: taskmolev1alpha1.WorkerProfileSpec{
			DisplayName: "Worker", Description: "A worker", Tags: []string{"test"},
			InputSchema: schema, OutputSchema: schema,
			Runtime: taskmolev1alpha1.WorkerRuntimeSpec{Image: "example.invalid/worker:v1", TimeoutSeconds: 60},
		},
	}
}
