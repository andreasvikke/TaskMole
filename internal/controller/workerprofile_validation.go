package controller

import (
	"fmt"
	"strings"

	taskmolev1alpha1 "github.com/andreasvikke/taskmole/api/v1alpha1"
	internalschema "github.com/andreasvikke/taskmole/internal/schema"
)

func validateProfile(profile *taskmolev1alpha1.WorkerProfile) (string, error) {
	if err := validateSchema("inputSchema", profile.Spec.InputSchema.Raw); err != nil {
		return taskmolev1alpha1.ReasonInvalidSchema, err
	}
	if err := validateSchema("outputSchema", profile.Spec.OutputSchema.Raw); err != nil {
		return taskmolev1alpha1.ReasonInvalidSchema, err
	}
	if err := validateRuntime(profile.Spec.Runtime); err != nil {
		return taskmolev1alpha1.ReasonInvalidRuntimeConfiguration, err
	}
	return taskmolev1alpha1.ReasonValid, nil
}

func validateSchema(field string, raw []byte) error {
	if len(raw) == 0 {
		return fmt.Errorf("%s must not be empty", field)
	}
	if len(raw) > taskmolev1alpha1.MaxSchemaBytes {
		return fmt.Errorf("%s exceeds the %d byte limit", field, taskmolev1alpha1.MaxSchemaBytes)
	}
	document, err := internalschema.Decode(raw)
	if err != nil {
		return fmt.Errorf("%s is not valid JSON: %w", field, err)
	}
	if err := internalschema.CheckReferences(document, 1, taskmolev1alpha1.MaxSchemaDepth); err != nil {
		return fmt.Errorf("%s: %w", field, err)
	}
	location := "urn:taskmole:" + field
	if _, err := internalschema.Compile(raw, location); err != nil {
		return fmt.Errorf("%s is not a valid Draft 2020-12 schema: %w", field, err)
	}
	return nil
}

func validateRuntime(runtime taskmolev1alpha1.WorkerRuntimeSpec) error {
	if strings.TrimSpace(runtime.Image) != runtime.Image || runtime.Image == "" {
		return fmt.Errorf("image must be non-empty and contain no surrounding whitespace")
	}
	return nil
}
