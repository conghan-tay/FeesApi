package fees

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

const (
	scaffoldWorkflowName = "fees.scaffold.workflow"
	scaffoldActivityName = "fees.scaffold.activity"
)

type workerRegistrar interface {
	RegisterWorkflowWithOptions(w interface{}, options workflow.RegisterOptions)
	RegisterActivityWithOptions(a interface{}, options activity.RegisterOptions)
}

func registerScaffoldWorker(registrar workerRegistrar) {
	registrar.RegisterWorkflowWithOptions(ScaffoldWorkflow, workflow.RegisterOptions{
		Name: scaffoldWorkflowName,
	})
	registrar.RegisterActivityWithOptions(ScaffoldActivity, activity.RegisterOptions{
		Name: scaffoldActivityName,
	})
}

func registerActivities(registrar workerRegistrar, activities *Activities) {
	registrar.RegisterActivityWithOptions(activities, activity.RegisterOptions{})
}

func ScaffoldWorkflow(ctx workflow.Context) error {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
	})
	return workflow.ExecuteActivity(activityCtx, scaffoldActivityName).Get(activityCtx, nil)
}

func ScaffoldActivity(ctx context.Context) error {
	return nil
}
