/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package disaggregatedset

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	disaggv1 "sigs.k8s.io/lws/api/disaggregatedset/v1"
)

// DisaggregatedSetRoleScalerWebhook validates DisaggregatedSetRoleScaler
// resources. It rejects malformed targetRefs, negative replica counts, and
// duplicate scalers targeting the same (DisaggregatedSet, role) pair.
type DisaggregatedSetRoleScalerWebhook struct {
	Client client.Client
}

// SetupDisaggregatedSetRoleScalerWebhook registers the webhook with the manager.
func SetupDisaggregatedSetRoleScalerWebhook(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &disaggv1.DisaggregatedSetRoleScaler{}).
		WithValidator(&DisaggregatedSetRoleScalerWebhook{Client: mgr.GetClient()}).
		Complete()
}

//+kubebuilder:webhook:path=/validate-disaggregatedset-x-k8s-io-v1-disaggregatedsetrolescaler,mutating=false,failurePolicy=fail,sideEffects=None,groups=disaggregatedset.x-k8s.io,resources=disaggregatedsetrolescalers,verbs=create;update,versions=v1,name=vdisaggregatedsetrolescaler.kb.io,admissionReviewVersions=v1

var _ admission.Validator[*disaggv1.DisaggregatedSetRoleScaler] = &DisaggregatedSetRoleScalerWebhook{}

// ValidateCreate implements admission.Validator for create operations.
func (w *DisaggregatedSetRoleScalerWebhook) ValidateCreate(ctx context.Context, scaler *disaggv1.DisaggregatedSetRoleScaler) (admission.Warnings, error) {
	allErrs := w.validate(ctx, scaler, nil)
	return nil, allErrs.ToAggregate()
}

// ValidateUpdate implements admission.Validator for update operations.
//
// The old object is passed to allow update-only rules if needed later (e.g.
// forbid changing spec.targetRef). Today the only difference from create is
// that the uniqueness check must exclude the object itself from the collision
// scan; validate() handles that via oldScaler.
func (w *DisaggregatedSetRoleScalerWebhook) ValidateUpdate(ctx context.Context, oldScaler, newScaler *disaggv1.DisaggregatedSetRoleScaler) (admission.Warnings, error) {
	allErrs := w.validate(ctx, newScaler, oldScaler)
	return nil, allErrs.ToAggregate()
}

// ValidateDelete implements admission.Validator for delete operations.
func (w *DisaggregatedSetRoleScalerWebhook) ValidateDelete(ctx context.Context, scaler *disaggv1.DisaggregatedSetRoleScaler) (admission.Warnings, error) {
	return nil, nil
}

// validate runs all validation rules; oldScaler is nil on create.
func (w *DisaggregatedSetRoleScalerWebhook) validate(
	ctx context.Context,
	scaler *disaggv1.DisaggregatedSetRoleScaler,
	oldScaler *disaggv1.DisaggregatedSetRoleScaler,
) field.ErrorList {
	var allErrs field.ErrorList

	specPath := field.NewPath("spec")
	targetPath := specPath.Child("targetRef")

	if scaler.Spec.TargetRef.Name == "" {
		allErrs = append(allErrs, field.Required(targetPath.Child("name"),
			"targetRef.name must reference a DisaggregatedSet in the same namespace"))
	}
	if scaler.Spec.TargetRef.Role == "" {
		allErrs = append(allErrs, field.Required(targetPath.Child("role"),
			"targetRef.role must reference a role of the referenced DisaggregatedSet"))
	}
	if scaler.Spec.Replicas != nil && *scaler.Spec.Replicas < 0 {
		allErrs = append(allErrs, field.Invalid(specPath.Child("replicas"),
			*scaler.Spec.Replicas, "must be non-negative"))
	}

	// Uniqueness: no other scaler in this namespace may share the same
	// (targetRef.name, targetRef.role) pair. Only run when the targetRef
	// itself is well-formed, to avoid noisy errors on already-invalid
	// admissions.
	if scaler.Spec.TargetRef.Name != "" && scaler.Spec.TargetRef.Role != "" && w.Client != nil {
		list := &disaggv1.DisaggregatedSetRoleScalerList{}
		if err := w.Client.List(ctx, list, client.InNamespace(scaler.Namespace)); err != nil {
			allErrs = append(allErrs, field.InternalError(specPath,
				fmt.Errorf("failed to list existing DisaggregatedSetRoleScalers: %w", err)))
		} else {
			for i := range list.Items {
				other := &list.Items[i]
				if other.Name == scaler.Name {
					continue
				}
				if oldScaler != nil && other.UID == oldScaler.UID {
					continue
				}
				if other.Spec.TargetRef.Name == scaler.Spec.TargetRef.Name &&
					other.Spec.TargetRef.Role == scaler.Spec.TargetRef.Role {
					allErrs = append(allErrs, field.Duplicate(targetPath,
						fmt.Sprintf("DisaggregatedSetRoleScaler %q already targets DisaggregatedSet %q role %q in this namespace",
							other.Name, scaler.Spec.TargetRef.Name, scaler.Spec.TargetRef.Role)))
					break
				}
			}
		}
	}

	return allErrs
}
