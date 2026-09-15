package controller

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	taskmolev1alpha1 "github.com/andreasvikke/taskmole/api/v1alpha1"
)

const (
	taskNameLabel         = "taskmole.io/task"
	profileAnnotation     = "taskmole.io/profile"
	idempotencyAnnotation = "taskmole.io/idempotency-hash"
	inputKey              = "input.json"
	workerContainerName   = "worker"
	workerUserID          = int64(65532)
)

// WorkerTaskReconciler schedules immutable task resources.
type WorkerTaskReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=taskmole.io,namespace=taskmole,resources=workertasks,verbs=get;list;watch
// +kubebuilder:rbac:groups=taskmole.io,namespace=taskmole,resources=workertasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",namespace=taskmole,resources=configmaps,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=batch,namespace=taskmole,resources=jobs,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",namespace=taskmole,resources=pods,verbs=get;list;watch

func (r *WorkerTaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	task := &taskmolev1alpha1.WorkerTask{}
	if err := r.Get(ctx, req.NamespacedName, task); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if isTerminal(task.Status.Phase) {
		return ctrl.Result{}, nil
	}
	configMap := desiredInputConfigMap(task)
	if err := controllerutil.SetControllerReference(task, configMap, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureResource(ctx, task, configMap); err != nil {
		return ctrl.Result{}, err
	}
	job := desiredJob(task)
	if err := controllerutil.SetControllerReference(task, job, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureResource(ctx, task, job); err != nil {
		return ctrl.Result{}, err
	}
	observedJob := &batchv1.Job{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(job), observedJob); err != nil {
		return ctrl.Result{}, err
	}
	pod, err := r.workerPod(ctx, task, observedJob)
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.projectLifecycle(ctx, task, observedJob, pod)
}

func (r *WorkerTaskReconciler) ensureResource(ctx context.Context, task *taskmolev1alpha1.WorkerTask, desired client.Object) error {
	current := desired.DeepCopyObject().(client.Object)
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), current)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	if !metav1.IsControlledBy(current, task) {
		return fmt.Errorf("refusing to adopt unexpected %T %s/%s", current, current.GetNamespace(), current.GetName())
	}
	return nil
}

func desiredInputConfigMap(task *taskmolev1alpha1.WorkerTask) *corev1.ConfigMap {
	immutable := true
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: task.Name + "-input", Namespace: task.Namespace,
		Labels: map[string]string{taskNameLabel: task.Name}, Annotations: taskAnnotations(task)}, Immutable: &immutable,
		BinaryData: map[string][]byte{inputKey: append([]byte(nil), task.Spec.Input.Raw...)}}
}

func desiredJob(task *taskmolev1alpha1.WorkerTask) *batchv1.Job {
	workerRuntime := task.Spec.ProfileSnapshot.Runtime
	runAsUser, runAsGroup := workerIdentity(workerRuntime)
	var hostUsers *bool
	if workerRuntime.ProcMount != nil && *workerRuntime.ProcMount == corev1.UnmaskedProcMount {
		hostUsers = pointer(false)
	}
	zero := int32(0)
	deadline := int64(workerRuntime.TimeoutSeconds)
	labels := map[string]string{taskNameLabel: task.Name}
	annotations := taskAnnotations(task)
	seccompProfile := workerRuntime.SeccompProfile.DeepCopy()
	if seccompProfile == nil {
		seccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}
	}
	container := corev1.Container{Name: workerContainerName, Image: workerRuntime.Image, ImagePullPolicy: workerRuntime.ImagePullPolicy,
		Command: append([]string(nil), workerRuntime.Command...), Args: append([]string(nil), workerRuntime.Args...),
		Env: append([]corev1.EnvVar(nil), workerRuntime.Env...), EnvFrom: append([]corev1.EnvFromSource(nil), workerRuntime.EnvFrom...),
		Resources: *workerRuntime.Resources.DeepCopy(),
		SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: pointer(false), RunAsUser: runAsUser, RunAsGroup: runAsGroup,
			Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}, ProcMount: workerRuntime.ProcMount},
		VolumeMounts: []corev1.VolumeMount{{Name: "input", MountPath: taskmolev1alpha1.InputPath, SubPath: inputKey, ReadOnly: true}}}
	container.Env = append(container.Env,
		corev1.EnvVar{Name: "TASKMOLE_INPUT_PATH", Value: taskmolev1alpha1.InputPath},
		corev1.EnvVar{Name: "TASKMOLE_RESULT_PATH", Value: taskmolev1alpha1.ResultPath})
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: task.Name, Namespace: task.Namespace, Labels: labels, Annotations: annotations}, Spec: batchv1.JobSpec{
		BackoffLimit: &zero, ActiveDeadlineSeconds: &deadline, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations}, Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever, ServiceAccountName: workerRuntime.ServiceAccountName, Containers: []corev1.Container{container},
			HostUsers:       hostUsers,
			SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: pointer(true), SeccompProfile: seccompProfile},
			Volumes: []corev1.Volume{{Name: "input", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: task.Name + "-input"}, Items: []corev1.KeyToPath{{Key: inputKey, Path: inputKey}}}}}},
		}}}}
}

func workerIdentity(runtime taskmolev1alpha1.WorkerRuntimeSpec) (*int64, *int64) {
	user, group := workerUserID, workerUserID
	if runtime.RunAsUser != nil {
		user = *runtime.RunAsUser
	}
	if runtime.RunAsGroup != nil {
		group = *runtime.RunAsGroup
	}
	return pointer(user), pointer(group)
}

func pointer[T any](value T) *T { return &value }

func taskAnnotations(task *taskmolev1alpha1.WorkerTask) map[string]string {
	return map[string]string{profileAnnotation: task.Spec.ProfileRef, idempotencyAnnotation: task.Spec.IdempotencyKeyHash}
}

func (r *WorkerTaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&taskmolev1alpha1.WorkerTask{}).Owns(&corev1.ConfigMap{}).Owns(&batchv1.Job{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, object client.Object) []reconcile.Request {
			name := object.GetLabels()[taskNameLabel]
			if name == "" {
				return nil
			}
			return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: object.GetNamespace(), Name: name}}}
		})).
		Named("workertask").Complete(r)
}
