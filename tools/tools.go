package tools

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/schollz/progressbar/v3"
)

// ----------------------------------------------------------------------------
// Constants and Core Types
// ----------------------------------------------------------------------------

const (
	TempBaseDir   = "temp"
	OutputBaseDir = "output"
)

// StatusType defines the possible status values for steps and the overall pipeline.
type StatusType string

const (
	StatusPending   StatusType = "Pending"
	StatusRunning   StatusType = "Running"
	StatusCompleted StatusType = "Completed"
	StatusFailed    StatusType = "Failed"
	StatusRetrying  StatusType = "Retrying" // Optional: if you plan to implement retries
	StatusSkipped   StatusType = "Skipped"  // Optional: if steps can be skipped
)

// StepStatus holds the status of an individual ETL step.
type StepStatus struct {
	StepName       string     `json:"stepName"`
	Status         StatusType `json:"status"`
	StartTime      time.Time  `json:"startTime"`
	EndTime        time.Time  `json:"endTime,omitempty"`
	DurationMillis int64      `json:"durationMillis,omitempty"`
	Message        string     `json:"message,omitempty"` // For errors or other info
}

// PipelineRun holds the overall status of an ETL pipeline execution.
type PipelineRun struct {
	RunID          string         `json:"runId"`
	StartTime      time.Time      `json:"startTime"`
	OverallStatus  StatusType     `json:"overallStatus"`
	Steps          []StepStatus   `json:"steps"`
	stepMap        map[string]int `json:"-"` // Internal map for quick step lookup by name
	StatusFilePath string         `json:"-"` // Runtime configuration, not part of saved JSON
}

// LoopState holds the progress of a loop processing records.
type LoopState struct {
	LastSuccessfullyProcessedIndex int `json:"lastSuccessfullyProcessedIndex"`
}

// RetryConfig holds parameters for retry logic.
// For a zero-valued RetryConfig, MaxRetries will be 0 and Delay will be 0s,
// effectively meaning no retries unless these fields are explicitly set.
type RetryConfig struct {
	MaxRetries int
	Delay      time.Duration
}

// RecordTransformer defines a function that takes a raw record and returns a transformed record or an error.
// The input `rawRecord` will be a pointer to the unmarshaled type (e.g., *MyInputType).
type RecordTransformer func(rawRecord interface{}) (transformedRecord interface{}, err error)

// RecordLoader defines a function that takes a transformed record and attempts to load it, returning an error.
type RecordLoader func(transformedRecord interface{}) (err error)

// StepContext holds context information for the currently executing step
type StepContext struct {
	StepName    string
	Encoder     *json.Encoder
	Closer      func() error
	FilePath    string
	Version     int
	TempEncoder *json.Encoder
	TempCloser  func() error
	TempFilePath string
}

// Global step context
var currentStepContext *StepContext

// ----------------------------------------------------------------------------
// Pipeline Lifecycle & Status Management
// ----------------------------------------------------------------------------

// NewPipelineRun initializes a new pipeline run.
func NewPipelineRun(statusFilePath string) *PipelineRun {
	return &PipelineRun{
		RunID:          fmt.Sprintf("run_%s", time.Now().Format("20060102_150405.000")),
		StartTime:      time.Now(),
		OverallStatus:  StatusPending,
		Steps:          make([]StepStatus, 0),
		stepMap:        make(map[string]int),
		StatusFilePath: statusFilePath,
	}
}

// ExecuteStep runs a single ETL step, handling status updates, logging, and error exiting.
// The pipeline will exit on the first error encountered in a step.
func (pr *PipelineRun) ExecuteStep(stepName string, stepFunc func() error) {
	pr.StartStep(stepName)
	
	// Initialize step context based on step name
	var stepType string
	switch stepName {
	case "ExtractUsers":
		stepType = "extract"
	case "MainLoop":
		stepType = "loop"
	case "LoadOutput":
		stepType = "load"
	default:
		stepType = "unknown"
	}
	
	// Initialize context if needed
	if stepType != "unknown" {
		if err := InitStepContext(stepName, stepType); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to initialize context for step '%s': %v\n", stepName, err)
			pr.EndStep(stepName, err)
			pr.LogStatus()
			os.Exit(1)
			return
		}
	}

	err := stepFunc()
	
	// Cleanup context
	if stepType != "unknown" {
		CleanupStepContext()
	}

	pr.EndStep(stepName, err)
	pr.LogStatus()

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error in step '%s': %v\n", stepName, err)
		if pr.StatusFilePath != "" {
			if saveErr := pr.SaveStatus(); saveErr != nil {
				fmt.Fprintf(os.Stderr, "Additionally, failed to save status to %s after error in step '%s': %v\n", pr.StatusFilePath, stepName, saveErr)
			}
		} else {
			fmt.Fprintf(os.Stderr, "Warning: StatusFilePath not set in PipelineRun, cannot save status after error in step '%s'.\n", stepName)
		}
		os.Exit(1)
	}
}

// StartStep marks a step as "Running" and records its start time.
// If the pipeline is "Pending", its status is updated to "Running".
func (pr *PipelineRun) StartStep(stepName string) {
	now := time.Now()
	if pr.OverallStatus == StatusPending {
		pr.OverallStatus = StatusRunning
	}

	if idx, exists := pr.stepMap[stepName]; exists {
		// Update existing step (e.g., for retries, though full retry logic isn't in ExecuteStep)
		pr.Steps[idx].Status = StatusRunning
		pr.Steps[idx].StartTime = now
		pr.Steps[idx].EndTime = time.Time{} // Clear previous end time
		pr.Steps[idx].DurationMillis = 0
		pr.Steps[idx].Message = ""
	} else {
		newStep := StepStatus{
			StepName:  stepName,
			Status:    StatusRunning,
			StartTime: now,
		}
		pr.Steps = append(pr.Steps, newStep)
		pr.stepMap[stepName] = len(pr.Steps) - 1
	}
}

// EndStep updates a step's status, records EndTime, Duration, and any error message.
// It also updates the pipeline's OverallStatus based on the outcome of this step.
func (pr *PipelineRun) EndStep(stepName string, err error) {
	now := time.Now()
	idx, exists := pr.stepMap[stepName]
	if !exists {
		// Should not happen if StartStep was called. Handle defensively.
		newStep := StepStatus{StepName: stepName, StartTime: now, EndTime: now}
		if err != nil {
			newStep.Status = StatusFailed
			newStep.Message = err.Error()
		} else {
			newStep.Status = StatusCompleted
		}
		pr.Steps = append(pr.Steps, newStep)
		pr.stepMap[stepName] = len(pr.Steps) - 1
		idx = len(pr.Steps) - 1 // Update index for subsequent logic
	}

	pr.Steps[idx].EndTime = now
	pr.Steps[idx].DurationMillis = pr.Steps[idx].EndTime.Sub(pr.Steps[idx].StartTime).Milliseconds()

	if err != nil {
		pr.Steps[idx].Status = StatusFailed
		pr.Steps[idx].Message = err.Error()
		pr.OverallStatus = StatusFailed // Any step failure makes the pipeline failed
	} else {
		pr.Steps[idx].Status = StatusCompleted
		// Only update overall status if not already failed
		if pr.OverallStatus != StatusFailed {
			allCompleted := true
			for _, step := range pr.Steps {
				if step.Status != StatusCompleted {
					allCompleted = false
					break
				}
			}
			if allCompleted {
				pr.OverallStatus = StatusCompleted
			} else {
				pr.OverallStatus = StatusRunning // Still other steps pending or running
			}
		}
	}
}

// LogStatus prints the current status of the pipeline and its steps to standard output.
func (pr *PipelineRun) LogStatus() {
	fmt.Printf("\n--- Pipeline Run Status ---\n")
	fmt.Printf("Run ID: %s\n", pr.RunID)
	fmt.Printf("Overall Status: %s\n", pr.OverallStatus)
	fmt.Printf("Start Time: %s\n", pr.StartTime.Format(time.RFC3339))

	if pr.OverallStatus == StatusCompleted || pr.OverallStatus == StatusFailed {
		var pipelineEndTime time.Time
		if len(pr.Steps) > 0 {
			// Determine pipeline end time from the latest step's end time
			for _, step := range pr.Steps {
				if step.EndTime.After(pipelineEndTime) {
					pipelineEndTime = step.EndTime
				}
			}
		}
		if !pipelineEndTime.IsZero() {
			fmt.Printf("End Time: %s\n", pipelineEndTime.Format(time.RFC3339))
			fmt.Printf("Total Duration: %s\n", pipelineEndTime.Sub(pr.StartTime).Round(time.Millisecond).String())
		}
	}

	fmt.Println("Steps:")
	if len(pr.Steps) == 0 {
		fmt.Println("  No steps initiated yet.")
	}
	for _, step := range pr.Steps {
		fmt.Printf("  - Step: %s\n", step.StepName)
		fmt.Printf("    Status: %s\n", step.Status)
		fmt.Printf("    Start Time: %s\n", step.StartTime.Format(time.RFC3339))
		if !step.EndTime.IsZero() {
			fmt.Printf("    End Time: %s\n", step.EndTime.Format(time.RFC3339))
			fmt.Printf("    Duration: %d ms\n", step.DurationMillis)
		}
		if step.Message != "" {
			fmt.Printf("    Message: %s\n", step.Message)
		}
	}
	fmt.Println("-------------------------")
}

// SaveStatus saves the PipelineRun struct as JSON to the file specified in pr.StatusFilePath.
func (pr *PipelineRun) SaveStatus() error {
	if pr.StatusFilePath == "" {
		return fmt.Errorf("StatusFilePath not set in PipelineRun, cannot save status")
	}
	data, err := json.MarshalIndent(pr, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal pipeline status: %w", err)
	}
	if err := EnsureDirForFile(pr.StatusFilePath); err != nil {
		return fmt.Errorf("failed to ensure directory for status file %s: %w", pr.StatusFilePath, err)
	}
	err = os.WriteFile(pr.StatusFilePath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write pipeline status to file %s: %w", pr.StatusFilePath, err)
	}
	fmt.Printf("Pipeline status saved to %s\n", pr.StatusFilePath)
	return nil
}

// Knoll performs initial setup tasks for the ETL pipeline:
// ensures the temporary directory exists and cleans it.
func Knoll() {
	fmt.Println("Starting ETL Pipeline...")
	if err := EnsureDir(TempBaseDir); err != nil {
		fmt.Fprintf(os.Stderr, "Critical Warning: could not ensure temp directory %s: %v. Proceeding with caution.\n", TempBaseDir, err)
	} else {
		if err := CleanDirContents(TempBaseDir, nil); err != nil { // Clean all for a fresh run
			fmt.Fprintf(os.Stderr, "Warning: could not clean temp directory %s: %v\n", TempBaseDir, err)
		}
	}
}

// Stow performs final tasks for a successfully completed ETL pipeline,
// primarily saving the final status.
func (pr *PipelineRun) Stow() {
	fmt.Println("ETL Pipeline completed successfully.")
	if err := pr.SaveStatus(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to save final successful status to %s: %v\n", pr.StatusFilePath, err)
	}
}

// ----------------------------------------------------------------------------
// File System & Path Utilities
// ----------------------------------------------------------------------------

// EnsureDir ensures that the specified directory path exists.
// If it does not exist, it creates it, including any necessary parents.
func EnsureDir(dirPath string) error {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dirPath, err)
	}
	return nil
}

// EnsureDirForFile ensures that the parent directory for the given file path exists.
func EnsureDirForFile(filePath string) error {
	dir := filepath.Dir(filePath)
	if dir == "." || dir == "" { // Current directory or no directory part
		return nil
	}
	return EnsureDir(dir)
}

// GetTempFilePath returns a path for a temporary file, structured under "temp/<stepName>/<fileName>".
// It ensures the subdirectory for the step exists.
func GetTempFilePath(stepName string, fileName string) (string, error) {
	if stepName == "" {
		return "", fmt.Errorf("stepName cannot be empty for temp file path")
	}
	if fileName == "" {
		return "", fmt.Errorf("fileName cannot be empty for temp file path")
	}
	dir := filepath.Join(TempBaseDir, stepName)
	if err := EnsureDir(dir); err != nil {
		return "", err
	}
	return filepath.Join(dir, fileName), nil
}

// CleanDirContents removes all files and subdirectories directly within dirPath,
// but not dirPath itself. Skips paths provided in the exceptions list (full paths).
func CleanDirContents(dirPath string, exceptions []string) error {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Directory doesn't exist, nothing to clean
		}
		return fmt.Errorf("failed to read directory %s: %w", dirPath, err)
	}

	exceptionSet := make(map[string]bool)
	for _, exc := range exceptions {
		exceptionSet[exc] = true
	}

	for _, entry := range entries {
		fullPath := filepath.Join(dirPath, entry.Name())
		if exceptionSet[fullPath] {
			continue
		}
		if err := os.RemoveAll(fullPath); err != nil {
			return fmt.Errorf("failed to remove %s: %w", fullPath, err)
		}
	}
	return nil
}

// getNextVersionNumber scans a directory for files named "N.jsonl"
// and returns the next available version number (max existing version + 1).
// If no versioned files exist, it returns 1.
func getNextVersionNumber(dirPath string) (int, error) {
	if err := EnsureDir(dirPath); err != nil {
		return 0, err
	}
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return 0, fmt.Errorf("failed to read directory %s for versioning: %w", dirPath, err)
	}

	maxVersion := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			name := entry.Name()
			if strings.HasSuffix(name, ".jsonl") {
				baseName := strings.TrimSuffix(name, ".jsonl")
				version, err := strconv.Atoi(baseName)
				if err == nil && version > maxVersion {
					maxVersion = version
				}
			}
		}
	}
	return maxVersion + 1, nil
}

// GetNextVersionedFilePath returns the path for the next versioned output file ("N.jsonl")
// under "output/<stepName>/<versionNumber>.jsonl".
// It also returns the determined version number.
func GetNextVersionedFilePath(stepName string) (string, int, error) {
	if stepName == "" {
		return "", 0, fmt.Errorf("stepName cannot be empty for versioned file path")
	}
	dir := filepath.Join(OutputBaseDir, stepName)
	if err := EnsureDir(dir); err != nil {
		return "", 0, err
	}

	version, err := getNextVersionNumber(dir)
	if err != nil {
		return "", 0, err
	}

	filePath := filepath.Join(dir, fmt.Sprintf("%d.jsonl", version))
	return filePath, version, nil
}

// GetLatestVersionedFilePath finds the path to the most recent versioned file ("N.jsonl")
// in "output/<stepName>/". Returns full file path and version number.
// Returns an error if no versioned files are found.
func GetLatestVersionedFilePath(stepName string) (string, int, error) {
	if stepName == "" {
		return "", 0, fmt.Errorf("stepName cannot be empty for latest versioned file path")
	}
	dir := filepath.Join(OutputBaseDir, stepName)

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", 0, fmt.Errorf("versioned directory %s does not exist", dir)
		}
		return "", 0, fmt.Errorf("failed to read directory %s for latest version: %w", dir, err)
	}

	latestVersion := 0
	found := false
	for _, entry := range entries {
		if !entry.IsDir() {
			name := entry.Name()
			if strings.HasSuffix(name, ".jsonl") {
				baseName := strings.TrimSuffix(name, ".jsonl")
				version, err := strconv.Atoi(baseName)
				if err == nil {
					if !found || version > latestVersion { // Ensure first valid version is taken
						latestVersion = version
						found = true
					}
				}
			}
		}
	}

	if !found {
		return "", 0, fmt.Errorf("no versioned .jsonl files found in %s", dir)
	}

	filePath := filepath.Join(dir, fmt.Sprintf("%d.jsonl", latestVersion))
	return filePath, latestVersion, nil
}

// GetSpecificVersionedFilePath returns the path for a specific versioned file:
// "output/<stepName>/<versionNumber>.jsonl".
func GetSpecificVersionedFilePath(stepName string, version int) (string, error) {
	if stepName == "" {
		return "", fmt.Errorf("stepName cannot be empty")
	}
	if version <= 0 {
		return "", fmt.Errorf("version number must be positive")
	}
	dir := filepath.Join(OutputBaseDir, stepName)
	filePath := filepath.Join(dir, fmt.Sprintf("%d.jsonl", version))
	return filePath, nil
}

// ----------------------------------------------------------------------------
// JSON & JSONL I/O Utilities
// ----------------------------------------------------------------------------

// SaveJSON marshals data to JSON and writes it to filePath, ensuring the directory exists.
func SaveJSON(filePath string, data interface{}) error {
	if err := EnsureDirForFile(filePath); err != nil {
		return err
	}
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON for %s: %w", filePath, err)
	}
	return os.WriteFile(filePath, jsonData, 0644)
}

// ReadJSON reads a JSON file from filePath and unmarshals it into target.
func ReadJSON(filePath string, target interface{}) error {
	jsonData, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read JSON file %s: %w", filePath, err)
	}
	return json.Unmarshal(jsonData, target)
}

// NewJSONLWriter opens/creates a file for writing line-delimited JSON.
// It returns a json.Encoder and a closer function.
// The caller is responsible for calling the closer function (e.g., via defer).
func NewJSONLWriter(filePath string) (encoder *json.Encoder, closer func() error, err error) {
	if err = EnsureDirForFile(filePath); err != nil {
		return nil, nil, fmt.Errorf("ensuring directory for %s: %w", filePath, err)
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("opening file %s for JSONL writing: %w", filePath, err)
	}

	encoder = json.NewEncoder(file)
	closer = file.Close
	return encoder, closer, nil
}

// GetNextVersionedJSONLWriter gets path for the next versioned .jsonl output file,
// returning a json.Encoder, a closer func, version number, and file path.
// If stepNameOpt is empty, it uses the caller function's name (of this function's caller) as the step name.
func GetNextVersionedJSONLWriter(stepNameOpt ...string) (encoder *json.Encoder, closer func() error, version int, filePath string, err error) {
	var actualStepName string
	if len(stepNameOpt) > 0 && stepNameOpt[0] != "" {
		actualStepName = stepNameOpt[0]
	} else {
		// Defaults to the name of the function that called GetNextVersionedJSONLWriter
		actualStepName = getCallerFunctionName(3)
	}

	filePath, version, err = GetNextVersionedFilePath(actualStepName)
	if err != nil {
		return nil, nil, 0, "", fmt.Errorf("getting next versioned .jsonl file path for %s: %w", actualStepName, err)
	}

	encoder, closer, err = NewJSONLWriter(filePath)
	if err != nil {
		return nil, nil, version, filePath, fmt.Errorf("creating JSONL writer for %s (version %d): %w", filePath, version, err)
	}
	return encoder, closer, version, filePath, nil
}

// StreamJSONLRecords reads a JSONL file line by line, unmarshaling each line
// into a new instance of recordTemplate's type, and calls onRecord for each.
// `recordTemplate` is used to determine the type; `onRecord` receives a pointer to the unmarshaled object.
func StreamJSONLRecords(filePath string, recordTemplate interface{}, onRecord func(recordPtr interface{}) error) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("opening file %s for JSONL reading: %w", filePath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	recordType := reflect.TypeOf(recordTemplate)
	if recordType.Kind() == reflect.Ptr {
		recordType = recordType.Elem() // Ensure we're working with the underlying struct type
	}

	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		lineBytes := scanner.Bytes()
		if len(strings.TrimSpace(string(lineBytes))) == 0 { // Skip empty lines
			continue
		}

		// Create a new pointer to an instance of the record type (e.g., *MyStruct)
		newRecordPtr := reflect.New(recordType).Interface()

		if err := json.Unmarshal(lineBytes, newRecordPtr); err != nil {
			return fmt.Errorf("unmarshaling line %d from %s: %w. Line content: %s", lineNumber, filePath, err, string(lineBytes))
		}

		if err := onRecord(newRecordPtr); err != nil {
			// Error from onRecord should indicate which record caused it, if possible.
			// Here we augment with line number context.
			return fmt.Errorf("processing record from line %d of %s: %w", lineNumber, filePath, err)
		}
	}

	return scanner.Err()
}

// ReadAllJSONLFile reads all records from a JSONL file into a slice.
// targetSlicePtr must be a pointer to a slice of the desired struct type (e.g., *[]MyStruct).
func ReadAllJSONLFile(filePath string, targetSlicePtr interface{}) error {
	slicePtrValue := reflect.ValueOf(targetSlicePtr)
	if slicePtrValue.Kind() != reflect.Ptr || slicePtrValue.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("targetSlicePtr must be a pointer to a slice, got %T", targetSlicePtr)
	}

	sliceValue := slicePtrValue.Elem()
	elemType := sliceValue.Type().Elem() // Type of the slice elements (e.g., MyStruct)

	// Reset slice before appending
	sliceValue.Set(reflect.MakeSlice(sliceValue.Type(), 0, 0))

	return StreamJSONLRecords(filePath, reflect.New(elemType).Elem().Interface(), func(recordPtr interface{}) error {
		// recordPtr is a pointer to an element (e.g., *MyStruct). We need its value for Append.
		elemValue := reflect.ValueOf(recordPtr).Elem()
		sliceValue.Set(reflect.Append(sliceValue, elemValue))
		return nil
	})
}

// ReadLatestVersionedJSONL reads the latest versioned JSONL file for a stepName,
// unmarshaling all records into targetSlicePtr. Returns the version read.
func ReadLatestVersionedJSONL(stepName string, targetSlicePtr interface{}) (int, error) {
	filePath, version, err := GetLatestVersionedFilePath(stepName)
	if err != nil {
		return 0, fmt.Errorf("getting latest versioned .jsonl file path for %s: %w", stepName, err)
	}

	err = ReadAllJSONLFile(filePath, targetSlicePtr)
	if err != nil {
		return version, fmt.Errorf("reading latest versioned .jsonl file %s: %w", filePath, err)
	}
	return version, nil
}

// ReadSpecificVersionedJSONL reads a specific versioned JSONL file,
// unmarshaling all records into targetSlicePtr.
func ReadSpecificVersionedJSONL(stepName string, version int, targetSlicePtr interface{}) error {
	filePath, err := GetSpecificVersionedFilePath(stepName, version)
	if err != nil {
		return fmt.Errorf("getting specific versioned .jsonl file path for %s v%d: %w", stepName, version, err)
	}

	err = ReadAllJSONLFile(filePath, targetSlicePtr)
	if err != nil {
		return fmt.Errorf("reading specific versioned .jsonl file %s: %w", filePath, err)
	}
	return nil
}

// ----------------------------------------------------------------------------
// Loop/Batch Processing Utilities
// ----------------------------------------------------------------------------

// GetLoopStateFilePath returns a standardized path for a loop's state file ("temp/<stepName>/loop_state.json").
func GetLoopStateFilePath(stepName string) (string, error) {
	if stepName == "" {
		return "", fmt.Errorf("stepName cannot be empty for loop state file path")
	}
	return GetTempFilePath(stepName, "loop_state.json")
}

// SaveLoopState saves the loop's current state to a JSON file.
func SaveLoopState(stateFilePath string, state LoopState) error {
	return SaveJSON(stateFilePath, state)
}

// LoadLoopState loads the loop's state from a JSON file.
// Returns a zero-valued LoopState (LastSuccessfullyProcessedIndex: -1) if file doesn't exist.
func LoadLoopState(stateFilePath string) (LoopState, error) {
	var state LoopState
	state.LastSuccessfullyProcessedIndex = -1 // Default: no records processed yet

	if _, err := os.Stat(stateFilePath); os.IsNotExist(err) {
		return state, nil // File doesn't exist, start fresh
	} else if err != nil {
		return state, fmt.Errorf("stating loop state file %s: %w", stateFilePath, err)
	}

	if err := ReadJSON(stateFilePath, &state); err != nil {
		return state, fmt.Errorf("reading loop state file %s: %w", stateFilePath, err)
	}
	return state, nil
}

// CleanLoopStateFile removes the loop state file. Ignores "not found" errors.
func CleanLoopStateFile(stateFilePath string) error {
	err := os.Remove(stateFilePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove state file %s: %w", stateFilePath, err)
	}
	return nil
}

// CountLines counts non-empty lines in a file.
func CountLines(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("opening file %s to count lines: %w", filePath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		if len(strings.TrimSpace(scanner.Text())) > 0 {
			count++
		}
	}
	if err := scanner.Err(); err != nil {
		return count, fmt.Errorf("scanning file %s to count lines: %w", filePath, err)
	}
	return count, nil
}

// ProcessStreamedRecords orchestrates processing records from inputFilePath.
// It handles state for resuming, progress reporting, retries for transform/load,
// and calls transformer and loader functions for each record.
// recordTemplate is an empty struct of the input type (e.g., MyType{}).
// retryCfgs is an optional RetryConfig; if not provided, no retries are performed.
func ProcessStreamedRecords(
	loopStepName string,
	inputFilePath string,
	recordTemplate interface{},
	transformer RecordTransformer,
	loader RecordLoader,
	retryCfgs ...RetryConfig,
) error {
	retryConfig := RetryConfig{MaxRetries: 0, Delay: 0} // Default: no retries
	if len(retryCfgs) > 0 {
		retryConfig = retryCfgs[0]
	}

	stateFilePath, err := GetLoopStateFilePath(loopStepName)
	if err != nil {
		return fmt.Errorf("%s: failed to get state file path: %w", loopStepName, err)
	}
	currentState, err := LoadLoopState(stateFilePath)
	if err != nil {
		return fmt.Errorf("%s: failed to load loop state from %s: %w", loopStepName, stateFilePath, err)
	}
	fmt.Printf("%s: Resuming from record index %d (0-based).\n", loopStepName, currentState.LastSuccessfullyProcessedIndex)

	totalRecords, err := CountLines(inputFilePath)
	if err != nil {
		return fmt.Errorf("%s: failed to count records in %s: %w", loopStepName, inputFilePath, err)
	}
	if totalRecords == 0 {
		fmt.Printf("%s: No records to process in %s.\n", loopStepName, inputFilePath)
		if currentState.LastSuccessfullyProcessedIndex == -1 { // Fresh state and empty file
			_ = CleanLoopStateFile(stateFilePath) // Best effort cleanup
		}
		return nil
	}

	progressMgr := NewLoopProgressManager(totalRecords, fmt.Sprintf("%s: Processing Records", loopStepName))
	defer progressMgr.Finish()

	if currentState.LastSuccessfullyProcessedIndex >= 0 {
		progressMgr.Set(currentState.LastSuccessfullyProcessedIndex + 1) // +1 because Set expects completed count
	}

	currentRecordIndex := -1 // 0-based index for records read from file
	var processingError error // To store the error that stops the loop

	streamErr := StreamJSONLRecords(inputFilePath, recordTemplate, func(recordPtr interface{}) error {
		currentRecordIndex++
		progressMgr.Describe(fmt.Sprintf("%s: Record %d/%d", loopStepName, currentRecordIndex+1, totalRecords))

		if currentRecordIndex <= currentState.LastSuccessfullyProcessedIndex {
			return nil // Skip already processed record
		}

		var transformedRecord interface{}
		var terr, lerr error
		operationSuccessful := false

		for attempt := 0; attempt <= retryConfig.MaxRetries; attempt++ {
			if attempt > 0 {
				fmt.Printf("\n%s: Retrying record index %d (attempt %d/%d) after %v delay...\n", loopStepName, currentRecordIndex, attempt, retryConfig.MaxRetries, retryConfig.Delay)
				time.Sleep(retryConfig.Delay)
				progressMgr.RenderBlank() // Re-render bar after custom print
			}

			transformedRecord, terr = transformer(recordPtr)
			if terr != nil {
				fmt.Printf("\n%s: Transform error for record index %d: %v\n", loopStepName, currentRecordIndex, terr)
				if attempt == retryConfig.MaxRetries {
					processingError = fmt.Errorf("%s: failed to transform record at index %d after %d retries: %w", loopStepName, currentRecordIndex, retryConfig.MaxRetries, terr)
					return processingError // Stop streaming
				}
				continue // Next attempt
			}

			lerr = loader(transformedRecord)
			if lerr != nil {
				fmt.Printf("\n%s: Load error for record index %d: %v\n", loopStepName, currentRecordIndex, lerr)
				if attempt == retryConfig.MaxRetries {
					processingError = fmt.Errorf("%s: failed to load record at index %d after %d retries: %w", loopStepName, currentRecordIndex, retryConfig.MaxRetries, lerr)
					return processingError // Stop streaming
				}
				continue // Next attempt
			}
			operationSuccessful = true
			break // Success
		}

		if !operationSuccessful {
			// This should ideally be caught by processingError above, but as a safeguard:
			if processingError == nil {
				processingError = fmt.Errorf("%s: unknown failure for record index %d after all retries", loopStepName, currentRecordIndex)
			}
			return processingError // Stop streaming
		}

		currentState.LastSuccessfullyProcessedIndex = currentRecordIndex
		if err := SaveLoopState(stateFilePath, currentState); err != nil {
			// This is a critical failure, as state is lost.
			fmt.Fprintf(os.Stderr, "\n%s: CRITICAL - Failed to save loop state after processing record %d: %v\n", loopStepName, currentRecordIndex, err)
			processingError = fmt.Errorf("%s: CRITICAL - failed to save loop state: %w", loopStepName, err)
			return processingError // Stop streaming
		}
		progressMgr.Add(1)
		return nil // Continue to next record
	})

	if streamErr != nil {
		// If processingError was set, streamErr will be processingError.
		// Otherwise, streamErr is from StreamJSONLRecords itself (e.g., file I/O).
		if processingError != nil {
			fmt.Fprintf(os.Stderr, "\n%s: Finished with error after processing %d records (index %d): %v\n", loopStepName, currentState.LastSuccessfullyProcessedIndex+1, currentState.LastSuccessfullyProcessedIndex, processingError)
			return processingError
		}
		return fmt.Errorf("%s: error during record streaming from %s: %w", loopStepName, inputFilePath, streamErr)
	}

	// Post-loop actions
	if currentState.LastSuccessfullyProcessedIndex == totalRecords-1 {
		fmt.Printf("\n%s: All %d records processed successfully. Cleaning up state file.\n", loopStepName, totalRecords)
		if err := CleanLoopStateFile(stateFilePath); err != nil {
			fmt.Fprintf(os.Stderr, "\n%s: Warning - failed to remove state file %s: %v\n", loopStepName, stateFilePath, err)
		}
	} else if totalRecords > 0 { // Loop finished, but not all records processed (and no error reported)
		// This case should ideally be covered by an error if the loop terminated prematurely.
		fmt.Fprintf(os.Stderr, "\n%s: Finished. Processed %d of %d records. Last successful index: %d. State file %s retained.\n",
			loopStepName, currentState.LastSuccessfullyProcessedIndex+1, totalRecords, currentState.LastSuccessfullyProcessedIndex, stateFilePath)
	}

	fmt.Printf("%s loop completed.\n", loopStepName)
	return nil
}

// ExecuteRecordProcessingLoop orchestrates processing records from the latest versioned input file
// produced by `inputStepName`. It uses `ProcessStreamedRecords` for the core logic.
func ExecuteRecordProcessingLoop(
	loopStepName string, // Name for this processing loop (for state, logging)
	inputStepName string, // StepName that produced the input JSONL file
	recordTemplate interface{},
	transformer RecordTransformer,
	loader RecordLoader,
	retryConfig RetryConfig,
) error {
	inputFilePath, version, err := GetLatestVersionedFilePath(inputStepName)
	if err != nil {
		return fmt.Errorf("%s: failed to get latest output from step '%s': %w", loopStepName, inputStepName, err)
	}
	fmt.Printf("%s: Processing input file: %s (version %d)\n", loopStepName, inputFilePath, version)

	return ProcessStreamedRecords(
		loopStepName,
		inputFilePath,
		recordTemplate,
		transformer,
		loader,
		retryConfig, // Pass as a single element to match variadic ProcessStreamedRecords
	)
}

// ----------------------------------------------------------------------------
// Generic Helper Creators (using Go Generics)
// ----------------------------------------------------------------------------

// CreateTransformer creates a RecordTransformer from a generic function.
// transformLogic takes a concrete type In and returns a concrete type Out.
// The returned RecordTransformer handles type assertion from interface{}.
func CreateTransformer[In, Out any](
	transformLogic func(record In) (Out, error),
) RecordTransformer {
	return func(rawRecord interface{}) (interface{}, error) {
		// rawRecord is expected to be a pointer to In (e.g., *MyStruct)
		// as StreamJSONLRecords provides a pointer.
		typedRecordPtr, ok := rawRecord.(*In)
		if !ok {
			// Attempt to handle if rawRecord is In (value type) directly, though less common from StreamJSONLRecords
			typedRecordVal, okVal := rawRecord.(In)
			if !okVal {
				var zeroIn In
				return nil, fmt.Errorf("transformer: unexpected type %T, expected *%T or %T", rawRecord, zeroIn, zeroIn)
			}
			return transformLogic(typedRecordVal) // Call with value
		}
		return transformLogic(*typedRecordPtr) // Call with dereferenced pointer
	}
}

// CreateLoader creates a RecordLoader from a generic function.
// loadLogic takes a concrete type In.
// The returned RecordLoader handles type assertion from interface{}.
func CreateLoader[In any](
	loadLogic func(record In) error,
) RecordLoader {
	return func(transformedRecord interface{}) error {
		// Transformed record could be a value or a pointer, depending on CreateTransformer's Out type.
		typedRecord, ok := transformedRecord.(In)
		if !ok {
			// If In is a struct type, transformedRecord might be *In
			typedRecordPtr, okPtr := transformedRecord.(*In)
			if !okPtr {
				var zeroIn In
				return fmt.Errorf("loader: unexpected type %T, expected %T or *%T", transformedRecord, zeroIn, zeroIn)
			}
			// If In itself is a pointer type (e.g. type In *MyStruct), this path might not be hit
			// or could lead to double pointers if not handled carefully in transformLogic.
			// Assuming In is mostly a struct type here.
			return loadLogic(*typedRecordPtr)
		}
		return loadLogic(typedRecord)
	}
}

// ----------------------------------------------------------------------------
// Progress Bar Utilities
// ----------------------------------------------------------------------------

// LoopProgressManager encapsulates progress bar logic.
type LoopProgressManager struct {
	bar          *progressbar.ProgressBar
	totalRecords int
}

// NewLoopProgressManager creates and initializes a progress bar.
// If totalRecords <= 0, a nil bar is used (no-op).
func NewLoopProgressManager(totalRecords int, description string) *LoopProgressManager {
	if totalRecords <= 0 {
		return &LoopProgressManager{totalRecords: totalRecords}
	}
	bar := progressbar.NewOptions(totalRecords,
		progressbar.OptionSetDescription(description),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionShowCount(),
		progressbar.OptionClearOnFinish(),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))
	return &LoopProgressManager{bar: bar, totalRecords: totalRecords}
}

// Set updates the progress bar to a specific count of completed items.
func (lpm *LoopProgressManager) Set(count int) {
	if lpm.bar != nil && count >= 0 {
		_ = lpm.bar.Set(count) // Error ignored as per original library examples for Set
	}
}

// Add increments the progress bar by n.
func (lpm *LoopProgressManager) Add(n int) {
	if lpm.bar != nil {
		_ = lpm.bar.Add(n) // Error ignored
	}
}

// Describe updates the progress bar's description text.
func (lpm *LoopProgressManager) Describe(desc string) {
	if lpm.bar != nil {
		lpm.bar.Describe(desc)
	}
}

// RenderBlank re-renders the bar, useful after printing other output to console.
func (lpm *LoopProgressManager) RenderBlank() {
	if lpm.bar != nil {
		lpm.bar.RenderBlank()
	}
}

// Finish closes/clears the progress bar.
func (lpm *LoopProgressManager) Finish() {
	if lpm.bar != nil {
		_ = lpm.bar.Finish() // Error from Finish() is typically not handled.
	}
}

// ----------------------------------------------------------------------------
// Internal Helpers
// ----------------------------------------------------------------------------

// getCallerFunctionName inspects the call stack to find the name of the calling function.
// `skip` is the number of stack frames to ascend:
//   - skip=0: runtime.Caller
//   - skip=1: getCallerFunctionName itself
//   - skip=2: the function that called getCallerFunctionName
//   - skip=3: the function that called the function that called getCallerFunctionName
//
// For example, if MyStep calls GetNextJSONLWriter which calls getCallerFunctionName(3),
// this function will attempt to return "MyStep".
func getCallerFunctionName(skip int) string {
	pc, _, _, ok := runtime.Caller(skip)
	if !ok {
		return "unknown_caller"
	}

	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return "unknown_function"
	}

	name := fn.Name() // e.g., "github.com/user/project/package.MyFunction" or "main.MyFunction"
	// Attempt to get just the function part after the last dot.
	if lastDot := strings.LastIndex(name, "."); lastDot != -1 {
		// Further check if there's a receiver type, e.g. "(*MyType).MyMethod"
		// This simple parsing might not cover all edge cases of function/method naming.
		name = name[lastDot+1:]
	}
	// Remove common receiver syntax like "(*MyType)" if method name is like MyType.MyMethod
	name = strings.TrimPrefix(name, "(*")
	if closingParen := strings.Index(name, ")"); closingParen != -1 && closingParen < len(name)-1 {
		name = name[closingParen+1:] // Assumes method name follows immediately
	}
	return name
}

// ----------------------------------------------------------------------------
// Simplified ETL Interface Functions
// ----------------------------------------------------------------------------

// InitStepContext initializes context for a step with automatic encoder setup
func InitStepContext(stepName string, stepType string) error {
	if currentStepContext != nil && currentStepContext.Closer != nil {
		currentStepContext.Closer()
	}
	
	currentStepContext = &StepContext{StepName: stepName}
	
	if stepType == "extract" {
		encoder, closer, version, filePath, err := GetNextVersionedJSONLWriter(stepName)
		if err != nil {
			return err
		}
		currentStepContext.Encoder = encoder
		currentStepContext.Closer = closer
		currentStepContext.FilePath = filePath
		currentStepContext.Version = version
		fmt.Printf("Extracting %s to version %d at %s\n", stepName, version, filePath)
	}
	
	return nil
}

// CleanupStepContext cleans up the current step context
func CleanupStepContext() {
	if currentStepContext != nil {
		if currentStepContext.Closer != nil {
			currentStepContext.Closer()
		}
		if currentStepContext.TempCloser != nil {
			currentStepContext.TempCloser()
		}
		currentStepContext = nil
	}
}

// GetCurrentEncoder returns the encoder for the current step context
func GetCurrentEncoder() *json.Encoder {
	if currentStepContext != nil {
		return currentStepContext.Encoder
	}
	return nil
}

// GetCurrentFilePath returns the file path for the current step context
func GetCurrentFilePath() string {
	if currentStepContext != nil {
		return currentStepContext.FilePath
	}
	return ""
}

// ProcessStreamedRecordsSimplified processes records with simplified interface
func ProcessStreamedRecordsSimplified(
	recordTemplate interface{},
	transformFunc interface{}, // Will be cast to appropriate type
	loadFunc interface{},      // Will be cast to appropriate type
) error {
	if currentStepContext == nil {
		return fmt.Errorf("no step context available")
	}
	
	stepName := currentStepContext.StepName
	fmt.Printf("Starting %s...\n", stepName)
	
	inputStepName := "ExtractUsers" // For MainLoop, input comes from ExtractUsers
	
	inputFilePath, _, err := GetLatestVersionedFilePath(inputStepName)
	if err != nil {
		return err
	}
	
	tempOutputFilePath, err := GetTempFilePath(stepName, "loaded_records.jsonl")
	if err != nil {
		return err
	}
	
	tempEncoder, tempCloser, err := NewJSONLWriter(tempOutputFilePath)
	if err != nil {
		return err
	}
	currentStepContext.TempEncoder = tempEncoder
	currentStepContext.TempCloser = tempCloser
	currentStepContext.TempFilePath = tempOutputFilePath
	
	// Create transformer that works with any function signature
	transformer := func(rawRecord interface{}) (interface{}, error) {
		// Use reflection to call the transform function
		transformVal := reflect.ValueOf(transformFunc)
		args := []reflect.Value{reflect.ValueOf(rawRecord).Elem()}
		results := transformVal.Call(args)
		
		if len(results) != 2 {
			return nil, fmt.Errorf("transform function must return (result, error)")
		}
		
		if !results[1].IsNil() {
			return nil, results[1].Interface().(error)
		}
		
		return results[0].Interface(), nil
	}
	
	// Create loader that works with any function signature
	loader := func(transformedRecord interface{}) error {
		loadVal := reflect.ValueOf(loadFunc)
		args := []reflect.Value{reflect.ValueOf(transformedRecord), reflect.ValueOf(tempEncoder)}
		results := loadVal.Call(args)
		
		if len(results) != 1 {
			return fmt.Errorf("load function must return error")
		}
		
		if !results[0].IsNil() {
			return results[0].Interface().(error)
		}
		
		return nil
	}
	
	err = ProcessStreamedRecords(
		stepName,
		inputFilePath,
		recordTemplate,
		transformer,
		loader,
	)
	
	if err != nil {
		// Attempt to remove partially written temp file on error
		if removeErr := os.Remove(tempOutputFilePath); removeErr != nil {
			fmt.Printf("Warning: failed to remove temp output file %s on error: %v\n", tempOutputFilePath, removeErr)
		}
		return fmt.Errorf("%s execution failed: %w", stepName, err)
	}
	
	fmt.Printf("%s completed. Temporary loaded data at: %s\n", stepName, tempOutputFilePath)
	return nil
}