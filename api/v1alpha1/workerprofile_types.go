package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	extensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// WorkerRuntimeSpec describes the Kubernetes container used to execute a worker.
type WorkerRuntimeSpec struct {
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`
	// +optional
	Command []string `json:"command,omitempty"`
	// +optional
	Args []string `json:"args,omitempty"`
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`
	// +optional
	EnvFrom []corev1.EnvFromSource `json:"envFrom,omitempty"`
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// SeccompProfile configures the seccomp profile applied to worker Pods. It defaults to RuntimeDefault.
	// +optional
	SeccompProfile *corev1.SeccompProfile `json:"seccompProfile,omitempty"`
	// RunAsUser is the numeric UID used by the worker container. It defaults to 65532.
	// +kubebuilder:validation:Minimum=1
	// +optional
	RunAsUser *int64 `json:"runAsUser,omitempty"`
	// RunAsGroup is the numeric GID used by the worker container. It defaults to 65532.
	// +kubebuilder:validation:Minimum=1
	// +optional
	RunAsGroup *int64 `json:"runAsGroup,omitempty"`
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=63
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`
	// +kubebuilder:validation:Minimum=60
	// +kubebuilder:validation:Maximum=86400
	TimeoutSeconds int32 `json:"timeoutSeconds"`
}

// WorkerProfileSpec defines the discovery contract and runtime for a worker.
type WorkerProfileSpec struct {
	// +kubebuilder:validation:MinLength=1
	DisplayName string `json:"displayName"`
	// +kubebuilder:validation:MinLength=1
	Description string `json:"description"`
	// +optional
	Tags []string `json:"tags,omitempty"`
	// InputSchema is a self-contained JSON Schema Draft 2020-12 document.
	InputSchema extensionsv1.JSON `json:"inputSchema"`
	// OutputSchema is a self-contained JSON Schema Draft 2020-12 document.
	OutputSchema extensionsv1.JSON `json:"outputSchema"`
	Runtime      WorkerRuntimeSpec `json:"runtime"`
}

// WorkerProfileStatus defines the observed state of WorkerProfile.
type WorkerProfileStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=wp
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// WorkerProfile is the Schema for the workerprofiles API.
type WorkerProfile struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitzero"`
	Spec              WorkerProfileSpec `json:"spec"`
	// +optional
	Status WorkerProfileStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true
// WorkerProfileList contains a list of WorkerProfile.
type WorkerProfileList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []WorkerProfile `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &WorkerProfile{}, &WorkerProfileList{})
		return nil
	})
}
