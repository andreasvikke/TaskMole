# TaskMole

<p align="center">
  <img src="docs/assets/taskmole-mascot.png" alt="TaskMole mascot" width="320">
</p>

TaskMole runs durable, long-running workers as Kubernetes Jobs. A caller can
invoke a worker, disconnect, and retrieve its validated result later through a
fresh MCP connection.

## Install

```sh
kubectl apply -f https://raw.githubusercontent.com/andreasvikke/taskmole/VERSION/dist/install.yaml
kubectl -n taskmole port-forward service/taskmole-mcp 8082:8082
```

The MCP endpoint has no authentication or TLS. Keep the Service internal and
use an authenticated TLS tunnel for external access.

Create a profile describing a worker image and its JSON contracts:

```yaml
apiVersion: taskmole.io/v1alpha1
kind: WorkerProfile
metadata:
  name: echo
  namespace: taskmole
spec:
  displayName: Echo
  description: Accepts a task and returns a completion message
  inputSchema:
    type: object
    required: [task]
    additionalProperties: false
    properties:
      task: {type: string, minLength: 1}
  outputSchema:
    type: object
    required: [message]
    additionalProperties: false
    properties:
      message: {const: I did your task}
  runtime:
    image: busybox:1.37
    command: [/bin/sh, -c]
    args:
      - echo '{"message":"I did your task"}' > "$TASKMOLE_RESULT_PATH"
    runAsUser: 65532
    runAsGroup: 65532
    timeoutSeconds: 300
```

The worker reads JSON from `$TASKMOLE_INPUT_PATH` (`/taskmole/input.json`) and
writes one JSON object of at most 4 KiB to `$TASKMOLE_RESULT_PATH`
(`/dev/termination-log`). Inputs are durable API data and must not contain
credentials; provide credentials through profile Secret references. Workers run
without privilege escalation or Linux capabilities. `runtime.runAsUser` and
`runtime.runAsGroup` select the numeric identity and each defaults to `65532`
when omitted. BusyBox works with this non-root identity for commands that do not
need privileged filesystem access.

TaskMole exposes exactly five tools: `list_workers`, `invoke_worker`,
`get_worker_task`, `list_worker_tasks`, and `list_running_worker_tasks`.

## Simple worker example

The sample profile uses BusyBox to accept the `task` input schema and write the
schema-valid `{"message":"I did your task"}` result. Install it directly:

```sh
kubectl apply -f config/samples/taskmole_v1alpha1_workerprofile.yaml
```

[`examples/simple-worker`](examples/simple-worker) also contains a complete Go
worker image for adapting into workers that need to parse or transform input.

Invoke `echo` through MCP with a unique idempotency key:

```json
{
  "profileName": "echo",
  "idempotencyKey": "example-task-001",
  "input": {
    "task": "Write a short status update"
  }
}
```

`invoke_worker` immediately returns a durable task ID. Passing that ID to
`get_worker_task` eventually returns:

```json
{
  "phase": "Succeeded",
  "result": {
    "message": "I did your task"
  }
}
```

## Development

Install Go 1.26, Go Task, kubectl, Kind, Kustomize, and a container runtime, or
use the optional environment with `devbox shell`.

```sh
task generate          # Regenerate CRDs, RBAC, and DeepCopy code
task verify-generated
task lint
task test              # Unit tests and envtest
task build
task test-e2e           # Isolated Kind acceptance test
```

Install a local image with `task install IMG=registry/taskmole:tag`. Remove the
application with `task uninstall`; use `task install-crds` and
`task uninstall-crds` when only CRDs should be managed.

Apache License 2.0. See [LICENSE](LICENSE).
