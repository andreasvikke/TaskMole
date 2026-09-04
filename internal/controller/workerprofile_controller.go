package controller

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	taskmolev1alpha1 "github.com/andreasvikke/taskmole/api/v1alpha1"
)

// WorkerProfileReconciler validates profiles and publishes their readiness.
type WorkerProfileReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=taskmole.io,namespace=taskmole,resources=workerprofiles,verbs=get;list;watch
// +kubebuilder:rbac:groups=taskmole.io,namespace=taskmole,resources=workerprofiles/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",namespace=taskmole,resources=events,verbs=create;patch

func (r *WorkerProfileReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	profile := &taskmolev1alpha1.WorkerProfile{}
	if err := r.Get(ctx, req.NamespacedName, profile); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	reason, validationErr := validateProfile(profile)
	condition := metav1.Condition{
		Type: taskmolev1alpha1.ConditionReady, Status: metav1.ConditionTrue,
		Reason: reason, Message: "WorkerProfile is valid", ObservedGeneration: profile.Generation,
	}
	if validationErr != nil {
		condition.Status = metav1.ConditionFalse
		condition.Message = validationErr.Error()
	}
	if profile.Status.ObservedGeneration == profile.Generation && conditionMatches(profile.Status.Conditions, condition) {
		return ctrl.Result{}, nil
	}

	base := profile.DeepCopy()
	profile.Status.ObservedGeneration = profile.Generation
	apiMeta.SetStatusCondition(&profile.Status.Conditions, condition)
	if err := r.Status().Patch(ctx, profile, client.MergeFrom(base)); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	log.FromContext(ctx).Info("Updated WorkerProfile readiness", "name", profile.Name, "ready", condition.Status)
	return ctrl.Result{}, nil
}

func conditionMatches(conditions []metav1.Condition, expected metav1.Condition) bool {
	current := apiMeta.FindStatusCondition(conditions, expected.Type)
	return current != nil && current.Status == expected.Status && current.Reason == expected.Reason &&
		current.Message == expected.Message && current.ObservedGeneration == expected.ObservedGeneration
}

func (r *WorkerProfileReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&taskmolev1alpha1.WorkerProfile{}).Named("workerprofile").Complete(r)
}
