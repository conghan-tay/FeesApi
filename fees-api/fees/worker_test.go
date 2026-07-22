package fees

import (
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

type recordingRegistrar struct {
	workflowNames []string
	activityNames []string
}

func (r *recordingRegistrar) RegisterWorkflowWithOptions(w interface{}, options workflow.RegisterOptions) {
	r.workflowNames = append(r.workflowNames, options.Name)
}

func (r *recordingRegistrar) RegisterActivityWithOptions(a interface{}, options activity.RegisterOptions) {
	r.activityNames = append(r.activityNames, options.Name)
}

func TestRegisterScaffoldWorker(t *testing.T) {
	registrar := &recordingRegistrar{}

	registerScaffoldWorker(registrar)

	if len(registrar.workflowNames) != 1 {
		t.Fatalf("registered %d workflows, want 1", len(registrar.workflowNames))
	}
	if registrar.workflowNames[0] != scaffoldWorkflowName {
		t.Fatalf("workflow name = %q, want %q", registrar.workflowNames[0], scaffoldWorkflowName)
	}
	if len(registrar.activityNames) != 1 {
		t.Fatalf("registered %d activities, want 1", len(registrar.activityNames))
	}
	if registrar.activityNames[0] != scaffoldActivityName {
		t.Fatalf("activity name = %q, want %q", registrar.activityNames[0], scaffoldActivityName)
	}
}
