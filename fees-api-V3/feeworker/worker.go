package feeworker

import (
	"encore.app/internal/feesworkflowcontract"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

type workerRegistrar interface {
	RegisterWorkflowWithOptions(w interface{}, options workflow.RegisterOptions)
	RegisterActivityWithOptions(a interface{}, options activity.RegisterOptions)
}

func registerWorkflows(registrar workerRegistrar) {
	registrar.RegisterWorkflowWithOptions(BillWorkflow, workflow.RegisterOptions{
		Name: feesworkflowcontract.BillWorkflowName,
	})
}

func registerActivities(registrar workerRegistrar, activities *Activities) {
	registrar.RegisterActivityWithOptions(activities, activity.RegisterOptions{})
}
