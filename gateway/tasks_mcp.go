package main

import (
	"context"
	"encoding/json"
)

// Keep task tools on the same SQL adapters as REST. Tokens are strings on the
// wire so clients cannot round an int64 fence through a JSON float.
var taskMCPTools = []mcpTool{
	{mcpToolDef{
		Name: "task_enqueue", Title: "Enqueue a task",
		Description: "Create an org-shared task in a queue. The queue/key pair is an idempotency key: identical retries return that task's current snapshot, changed content conflicts. Dependencies must be existing tasks in your org. Declare producer-selected memory inputs and their exact versions; completion checks that they remain readable and unchanged. Returns {task,replayed}.",
		InputSchema: object(map[string]any{
			"queue":        taskStringSchema("Queue in your organization.", 128),
			"key":          taskStringSchema("Stable enqueue key within the queue.", 256),
			"payload":      map[string]any{"description": "Task input payload, any JSON value, at most 64 KiB."},
			"dependencies": map[string]any{"type": "array", "maxItems": 64, "items": taskIDSchema("Existing task ID.")},
			"inputs": map[string]any{"type": "array", "maxItems": 64, "items": object(map[string]any{
				"agent_id": taskIDSchema("Memory producer agent ID."),
				"key":      taskStringSchema("Memory key.", 256),
				"version":  map[string]any{"type": "integer", "minimum": 1, "description": "The version read from this producer."},
			}, "agent_id", "key", "version")},
		}, "queue", "key", "payload"),
		Annotations: annotations("Enqueue a task", false, false, true),
	}, toolTaskEnqueue},
	{mcpToolDef{
		Name: "task_get", Title: "Read a task",
		Description: "Read a task and its current claim, lease, fencing token and result. Only tasks in your organization are visible.",
		InputSchema: object(map[string]any{"id": taskIDSchema("Task ID.")}, "id"),
		Annotations: annotations("Read a task", true, false, true),
	}, toolTaskGet},
	{mcpToolDef{
		Name: "task_claim", Title: "Claim an eligible task",
		Description: "Atomically claim one ready task or reclaim an expired lease after all dependencies complete. Returns {task,replayed}, where task is null if none is eligible. Use a new request_id UUID for each claim attempt and reuse that ID after an uncertain response: an identical retry returns the original snapshot without claiming another task. A replayed lease may already have expired; all writes check the current lease and fence.",
		InputSchema: object(map[string]any{
			"queue":      taskStringSchema("Queue in your organization.", 128),
			"request_id": taskIDSchema("Stable UUID for this claim attempt; reuse only for identical retries."),
			"lease_ms":   taskLeaseSchema(),
		}, "queue", "request_id"),
		Annotations: annotations("Claim an eligible task", false, false, true),
	}, toolTaskClaim},
	{mcpToolDef{
		Name: "task_renew", Title: "Renew a task lease",
		Description: "Renew your current unexpired lease using database time. A stale fence, different owner or expired lease is refused. Renewal cannot revive an expired claim.",
		InputSchema: object(map[string]any{"id": taskIDSchema("Task ID."), "fence": taskFenceSchema(), "lease_ms": taskLeaseSchema()}, "id", "fence"),
		Annotations: annotations("Renew a task lease", false, false, false),
	}, toolTaskRenew},
	{mcpToolDef{
		Name: "task_release", Title: "Release a task claim",
		Description: "Release your current unexpired claim so another worker may acquire the task. A later claim receives a new fence; your old fence cannot complete it.",
		InputSchema: object(map[string]any{"id": taskIDSchema("Task ID."), "fence": taskFenceSchema()}, "id", "fence"),
		Annotations: annotations("Release a task claim", false, false, false),
	}, toolTaskRelease},
	{mcpToolDef{
		Name: "task_complete", Title: "Complete a task atomically",
		Description: "Store a result and complete the task atomically only while you own its unexpired current fence and every declared memory input remains readable at the declared version. An identical result retry under the successful owner/fence returns the completed receipt, even after its lease expires; changed results conflict. This fences PostgreSQL state, not external effects. Returns {task,replayed}.",
		InputSchema: object(map[string]any{
			"id": taskIDSchema("Task ID."), "fence": taskFenceSchema(),
			"result": map[string]any{"description": "Complete task result, any JSON value, at most 64 KiB."},
		}, "id", "fence", "result"),
		Annotations: annotations("Complete a task atomically", false, false, true),
	}, toolTaskComplete},
	{mcpToolDef{
		Name: "task_cancel", Title: "Cancel a task",
		Description: "Cancel a non-completed task you created, invalidating any current claim. Other org members cannot cancel your tasks.",
		InputSchema: object(map[string]any{"id": taskIDSchema("Task ID.")}, "id"),
		Annotations: annotations("Cancel a task", false, true, false),
	}, toolTaskCancel},
	{mcpToolDef{
		Name: "task_history", Title: "Read task history",
		Description: "Read the newest task state transitions with attribution and monotonically increasing per-task revisions. Returns {events}; task history is read-only and scoped to your org.",
		InputSchema: object(map[string]any{
			"id":    taskIDSchema("Task ID."),
			"limit": map[string]any{"type": "integer", "minimum": 1, "maximum": 1000, "default": 50},
		}, "id"),
		Annotations: annotations("Read task history", true, false, true),
	}, toolTaskHistory},
}

func taskStringSchema(description string, max int) map[string]any {
	return map[string]any{"type": "string", "minLength": 1, "maxLength": max, "description": description}
}

func taskIDSchema(description string) map[string]any {
	return map[string]any{"type": "string", "format": "uuid", "description": description}
}

func taskFenceSchema() map[string]any {
	return map[string]any{"type": "string", "pattern": "^[1-9][0-9]*$", "description": "Exact decimal int64 fencing token returned by task_claim; keep it as a string."}
}

func taskLeaseSchema() map[string]any {
	return map[string]any{"type": "integer", "minimum": 100, "maximum": 300000, "default": 30000, "description": "Lease duration measured from database time, in milliseconds."}
}

func taskToolResult(out json.RawMessage, err *apiError) map[string]any {
	if err != nil {
		return toolError(err)
	}
	return toolJSON(out)
}

func toolTaskEnqueue(g *Gateway, ctx context.Context, a *Agent, args json.RawMessage) map[string]any {
	var in taskEnqueueRequest
	if err := decodeTaskArgs(args, &in); err != nil {
		return toolError(err)
	}
	return taskToolResult(g.taskEnqueue(ctx, a, in))
}

func toolTaskGet(g *Gateway, ctx context.Context, a *Agent, args json.RawMessage) map[string]any {
	var in struct {
		ID string `json:"id"`
	}
	if err := decodeTaskArgs(args, &in); err != nil {
		return toolError(err)
	}
	return taskToolResult(g.taskGet(ctx, a, in.ID))
}

func toolTaskClaim(g *Gateway, ctx context.Context, a *Agent, args json.RawMessage) map[string]any {
	var in taskClaimRequest
	if err := decodeTaskArgs(args, &in); err != nil {
		return toolError(err)
	}
	return taskToolResult(g.taskClaim(ctx, a, in))
}

func toolTaskRenew(g *Gateway, ctx context.Context, a *Agent, args json.RawMessage) map[string]any {
	var in struct {
		ID string `json:"id"`
		taskRenewRequest
	}
	if err := decodeTaskArgs(args, &in); err != nil {
		return toolError(err)
	}
	return taskToolResult(g.taskRenew(ctx, a, in.ID, in.taskRenewRequest))
}

func toolTaskRelease(g *Gateway, ctx context.Context, a *Agent, args json.RawMessage) map[string]any {
	var in struct {
		ID string `json:"id"`
		taskFenceRequest
	}
	if err := decodeTaskArgs(args, &in); err != nil {
		return toolError(err)
	}
	return taskToolResult(g.taskRelease(ctx, a, in.ID, in.Fence))
}

func toolTaskComplete(g *Gateway, ctx context.Context, a *Agent, args json.RawMessage) map[string]any {
	var in struct {
		ID string `json:"id"`
		taskCompleteRequest
	}
	if err := decodeTaskArgs(args, &in); err != nil {
		return toolError(err)
	}
	return taskToolResult(g.taskComplete(ctx, a, in.ID, in.taskCompleteRequest))
}

func toolTaskCancel(g *Gateway, ctx context.Context, a *Agent, args json.RawMessage) map[string]any {
	var in struct {
		ID string `json:"id"`
	}
	if err := decodeTaskArgs(args, &in); err != nil {
		return toolError(err)
	}
	return taskToolResult(g.taskCancel(ctx, a, in.ID))
}

func toolTaskHistory(g *Gateway, ctx context.Context, a *Agent, args json.RawMessage) map[string]any {
	var in struct {
		ID    string `json:"id"`
		Limit *int   `json:"limit,omitempty"`
	}
	if err := decodeTaskArgs(args, &in); err != nil {
		return toolError(err)
	}
	return taskToolResult(g.taskHistory(ctx, a, in.ID, in.Limit))
}
