package fees

import (
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

type workerRegistrar interface {
	RegisterWorkflowWithOptions(w interface{}, options workflow.RegisterOptions)
	RegisterActivityWithOptions(a interface{}, options activity.RegisterOptions)
}

func registerWorkflows(registrar workerRegistrar) {
	registrar.RegisterWorkflowWithOptions(BillWorkflow, workflow.RegisterOptions{
		Name: BillWorkflowName,
	})
}

func registerActivities(registrar workerRegistrar, activities *Activities) {
	registrar.RegisterActivityWithOptions(activities, activity.RegisterOptions{})
}
