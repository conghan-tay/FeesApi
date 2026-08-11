package fees

import (
	"reflect"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

type recordingRegistrar struct {
	workflowNames []string
	activityNames []string
	activities    []interface{}
}

func (r *recordingRegistrar) RegisterWorkflowWithOptions(w interface{}, options workflow.RegisterOptions) {
	r.workflowNames = append(r.workflowNames, options.Name)
}

func (r *recordingRegistrar) RegisterActivityWithOptions(a interface{}, options activity.RegisterOptions) {
	r.activityNames = append(r.activityNames, options.Name)
	r.activities = append(r.activities, a)
}

func TestRegisterWorkflows(t *testing.T) {
	registrar := &recordingRegistrar{}

	registerWorkflows(registrar)

	if len(registrar.workflowNames) != 1 {
		t.Fatalf("registered %d workflows, want 1", len(registrar.workflowNames))
	}
	if registrar.workflowNames[0] != BillWorkflowName {
		t.Fatalf("workflow name = %q, want %q", registrar.workflowNames[0], BillWorkflowName)
	}
	if len(registrar.activityNames) != 0 {
		t.Fatalf("registered %d activities, want 0", len(registrar.activityNames))
	}
}

func TestRegisterActivities(t *testing.T) {
	registrar := &recordingRegistrar{}
	activities := NewActivities()

	registerActivities(registrar, activities)

	if len(registrar.activityNames) != 1 {
		t.Fatalf("registered %d activity batches, want 1", len(registrar.activityNames))
	}
	if registrar.activityNames[0] != "" {
		t.Fatalf("activity registration name = %q, want empty name for unprefixed method activities", registrar.activityNames[0])
	}
	if len(registrar.activities) != 1 {
		t.Fatalf("registered %d activity values, want 1", len(registrar.activities))
	}
	if registrar.activities[0] != activities {
		t.Fatal("registered activity value is not the Activities instance passed to registerActivities")
	}
	if got := reflect.TypeOf(registrar.activities[0]).String(); got != "*fees.Activities" {
		t.Fatalf("registered activity type = %q, want *fees.Activities", got)
	}
	activityType := reflect.TypeOf(registrar.activities[0])
	if _, ok := activityType.MethodByName("ActivityPersistBill"); ok {
		t.Fatal("ActivityPersistBill remains registered")
	}
	if _, ok := activityType.MethodByName("ActivityPersistInvoice"); ok {
		t.Fatal("ActivityPersistInvoice remains registered")
	}
	for _, name := range []string{"ActivityPublishPending", "ActivityPublishFinalized", "ActivityLongRunning", "ActivityAutoCloseBill"} {
		if _, ok := activityType.MethodByName(name); !ok {
			t.Fatalf("%s is not available on registered Activities", name)
		}
	}
}
