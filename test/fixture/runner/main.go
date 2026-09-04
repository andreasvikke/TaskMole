// Command runner is TaskMole's deterministic end-to-end fixture worker.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type input struct {
	Mode    string `json:"mode"`
	Message string `json:"message"`
}

func main() {
	raw, err := os.ReadFile(os.Getenv("TASKMOLE_INPUT_PATH"))
	if err != nil {
		fail(err)
	}
	var request input
	if err := json.Unmarshal(raw, &request); err != nil {
		fail(err)
	}
	switch request.Mode {
	case "success":
		writeResult(map[string]any{"acknowledged": true, "message": request.Message})
	case "failure":
		os.Exit(7)
	case "timeout":
		time.Sleep(2 * time.Minute)
	case "invalid":
		if err := os.WriteFile(os.Getenv("TASKMOLE_RESULT_PATH"), []byte("not-json"), 0o600); err != nil {
			fail(err)
		}
	case "missing":
		return
	default:
		fail(fmt.Errorf("unknown mode %q", request.Mode))
	}
}

func writeResult(result any) {
	raw, err := json.Marshal(result)
	if err != nil {
		fail(err)
	}
	if err := os.WriteFile(os.Getenv("TASKMOLE_RESULT_PATH"), raw, 0o600); err != nil {
		fail(err)
	}
}

func fail(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
