package v1alpha1

import (
	extensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// WorkerProfileSnapshot freezes the profile contract used for one invocation.
type WorkerProfileSnapshot struct {
	InputSchema  extensionsv1.JSON `json:"inputSchema"`
	OutputSchema extensionsv1.JSON `json:"outputSchema"`
	Runtime      WorkerRuntimeSpec `json:"runtime"`
}

// WorkerTaskSpec defines an immutable, durable worker invocation.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable"
type WorkerTaskSpec struct {
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	// +kubebuilder:validation:MaxLength=253
	ProfileRef string `json:"profileRef"`
	// IdempotencyKeyHash is the lowercase SHA-256 hash of the caller's key.
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	IdempotencyKeyHash string `json:"idempotencyKeyHash"`
	// Input is durable Kubernetes API data and must not contain credentials.
	Input           extensionsv1.JSON     `json:"input"`
	ProfileSnapshot WorkerProfileSnapshot `json:"profileSnapshot"`
}

// WorkerTaskStatus defines the durable observed state of WorkerTask.
type WorkerTaskStatus struct {
	// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed;TimedOut
	// +optional
	Phase WorkerTaskPhase `json:"phase,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	CreatedAt *metav1.Time `json:"createdAt,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
	// +optional
	JobName string `json:"jobName,omitempty"`
	// +optional
	PodName string `json:"podName,omitempty"`
	// +optional
	ExitCode *int32 `json:"exitCode,omitempty"`
	// +optional
	Reason string `json:"reason,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	Result *extensionsv1.JSON `json:"result,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=wt
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Profile",type=string,JSONPath=".spec.profileRef"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// WorkerTask is the Schema for the workertasks API.
type WorkerTask struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitzero"`
	Spec              WorkerTaskSpec `json:"spec"`
	// +optional
	Status WorkerTaskStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true
// WorkerTaskList contains a list of WorkerTask.
type WorkerTaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []WorkerTask `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &WorkerTask{}, &WorkerTaskList{})
		return nil
	})
}
