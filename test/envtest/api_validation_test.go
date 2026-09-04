package envtest_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	taskmolev1alpha1 "github.com/andreasvikke/taskmole/api/v1alpha1"
	extensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"
)

var (
	k8sClient client.Client
	testEnv   *envtest.Environment
)

const testNamespace = "default"

func TestMain(m *testing.M) {
	scheme := runtime.NewScheme()
	if err := taskmolev1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	testEnv = &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")}}
	cfg, err := testEnv.Start()
	if err != nil {
		panic(err)
	}
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		panic(err)
	}

	code := m.Run()
	if err := testEnv.Stop(); err != nil {
		panic(err)
	}
	os.Exit(code)
}

func TestCRDValidation(t *testing.T) {
	ctx := context.Background()

	missingRequired := &taskmolev1alpha1.WorkerProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "missing", Namespace: testNamespace},
	}
	if err := k8sClient.Create(ctx, missingRequired); !apierrors.IsInvalid(err) {
		t.Fatalf("missing required fields: got %v, want Invalid", err)
	}

	badTimeout := validProfile("bad-timeout")
	badTimeout.Spec.Runtime.TimeoutSeconds = 59
	if err := k8sClient.Create(ctx, badTimeout); !apierrors.IsInvalid(err) {
		t.Fatalf("timeout below minimum: got %v, want Invalid", err)
	}

	badReference := validProfile("bad-reference")
	badReference.Spec.Runtime.ServiceAccountName = "Not_DNS_Safe"
	if err := k8sClient.Create(ctx, badReference); !apierrors.IsInvalid(err) {
		t.Fatalf("invalid service account reference: got %v, want Invalid", err)
	}
}

func TestWorkerTaskSpecIsImmutable(t *testing.T) {
	ctx := context.Background()
	task := validTask("immutable")
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create valid task: %v", err)
	}
	task.Spec.ProfileRef = "other"
	if err := k8sClient.Update(ctx, task); !apierrors.IsInvalid(err) {
		t.Fatalf("mutate task spec: got %v, want Invalid", err)
	}
}

func TestExamplesPassAPIServerValidation(t *testing.T) {
	tests := []struct {
		path string
		into client.Object
	}{
		{path: "taskmole_v1alpha1_workerprofile.yaml", into: &taskmolev1alpha1.WorkerProfile{}},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			contents, err := os.ReadFile(filepath.Join("..", "..", "config", "samples", tc.path))
			if err != nil {
				t.Fatalf("read example: %v", err)
			}
			jsonData, err := yaml.YAMLToJSON(contents)
			if err != nil {
				t.Fatalf("convert example: %v", err)
			}
			if err := json.Unmarshal(jsonData, tc.into); err != nil {
				t.Fatalf("decode example: %v", err)
			}
			tc.into.SetNamespace(testNamespace)
			if err := k8sClient.Create(context.Background(), tc.into); err != nil {
				t.Fatalf("server-side validation failed: %v", err)
			}
		})
	}
}

func validProfile(name string) *taskmolev1alpha1.WorkerProfile {
	schema := extensionsv1.JSON{Raw: []byte(`{"type":"object"}`)}
	return &taskmolev1alpha1.WorkerProfile{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: taskmolev1alpha1.WorkerProfileSpec{
			DisplayName: "Test worker", Description: "Used by API contract tests",
			InputSchema: schema, OutputSchema: schema,
			Runtime: taskmolev1alpha1.WorkerRuntimeSpec{Image: "example.invalid/worker:v1", TimeoutSeconds: 60},
		},
	}
}

func validTask(name string) *taskmolev1alpha1.WorkerTask {
	schema := extensionsv1.JSON{Raw: []byte(`{"type":"object"}`)}
	return &taskmolev1alpha1.WorkerTask{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: taskmolev1alpha1.WorkerTaskSpec{
			ProfileRef: "test", IdempotencyKeyHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Input: extensionsv1.JSON{Raw: []byte(`{"message":"hello"}`)},
			ProfileSnapshot: taskmolev1alpha1.WorkerProfileSnapshot{
				InputSchema: schema, OutputSchema: schema,
				Runtime: taskmolev1alpha1.WorkerRuntimeSpec{Image: "example.invalid/worker:v1", TimeoutSeconds: 60},
			},
		},
	}
}
