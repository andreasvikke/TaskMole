package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type input struct {
	Task string `json:"task"`
}

type output struct {
	Message string `json:"message"`
}

func main() {
	inputPath := os.Getenv("TASKMOLE_INPUT_PATH")
	resultPath := os.Getenv("TASKMOLE_RESULT_PATH")
	if inputPath == "" || resultPath == "" {
		fail("TaskMole file paths are not configured")
	}

	raw, err := os.ReadFile(inputPath)
	if err != nil {
		fail("Could not read task input: %v", err)
	}
	var request input
	if err := json.Unmarshal(raw, &request); err != nil {
		fail("Could not decode task input: %v", err)
	}
	if request.Task == "" {
		fail("Task must not be empty")
	}

	result, err := json.Marshal(output{Message: "I did your task"})
	if err != nil {
		fail("Could not encode task result: %v", err)
	}
	if err := os.WriteFile(resultPath, result, 0o600); err != nil {
		fail("Could not write task result: %v", err)
	}
}

func fail(format string, values ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}
