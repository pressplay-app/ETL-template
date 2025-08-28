package main

import (
	"encoding/json"
	"fmt"
	"os" // Keep for Fprintf if used directly
	"time"

	"github.com/arbirk/ETL-template/tools"
)

// UserData defines the structure of our user records
type UserData struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// TransformedUserData defines the structure after transformation
type TransformedUserData struct {
	UserID        string `json:"userId"`
	ProcessedName string `json:"processedName"`
	Timestamp     string `json:"timestamp"`
}

//goetl:type=extract versioned next=MainLoop
func ExtractUsers() error {
	fmt.Println("Extracting users...")

	encoder, closer, version, filePath, err := tools.GetNextVersionedJSONLWriter("ExtractUsers") // Pass step name directly
	if err != nil {
		return err
	}
	defer closer()
	fmt.Printf("Extracting users to version %d at %s\n", version, filePath)

	users := []UserData{
		{ID: "4", Name: "Karen"},
		{ID: "5", Name: "Kevin"},
		{ID: "10", Name: "Bad User Transform"}, // To test transform failure
		{ID: "6", Name: "Steven"},
		{ID: "11", Name: "Bad User Load"}, // To test load failure (if transform succeeds)
		{ID: "7", Name: "Laura"},
	}
	for _, user := range users {
		if err := encoder.Encode(user); err != nil {
			return fmt.Errorf("encoding user %+v to %s: %w", user, filePath, err)
		}
	}
	fmt.Printf("Users extracted successfully to %s (Version %d)\n", filePath, version)
	return nil
}

// transformSingleRecord transforms a single UserData record.
func transformSingleRecord(user UserData) (TransformedUserData, error) {
	return TransformedUserData{
		UserID:        user.ID,
		ProcessedName: fmt.Sprintf("Processed_%s_Individually", user.Name),
		Timestamp:     time.Now().Format(time.RFC3339Nano),
	}, nil
}

// loadSingleRecord "loads" a single TransformedUserData record by writing it to the provided encoder.
func loadSingleRecord(tUser TransformedUserData, encoder *json.Encoder) error {
	err := encoder.Encode(tUser)
	if err != nil {
		return err
	}
	return nil
}

//goetl:type=loop versioned_input=extract/ExtractUsers next=FinalSummary (example next step)
func MainLoop() error {
	fmt.Println("Starting MainLoop...")
	loopStepName := "MainLoop"
	inputProducerStepName := "ExtractUsers"
	inputFilePath, _, err := tools.GetLatestVersionedFilePath(inputProducerStepName)
	tempOutputFilePath, err := tools.GetTempFilePath(loopStepName, "loaded_records.jsonl") // Fixed parenthesis
	tempEncoder, tempCloser, err := tools.NewJSONLWriter(tempOutputFilePath)
	defer tempCloser() // Ensure closer is called

	err = tools.ProcessStreamedRecords(
		loopStepName,
		inputFilePath,
		UserData{},
		tools.CreateTransformer(func(ud *UserData) (TransformedUserData, error) { return transformSingleRecord(*ud) }),
		tools.CreateLoader(func(tud TransformedUserData) error { return loadSingleRecord(tud, tempEncoder) }),
	)

	if err != nil {
		// Attempt to remove partially written temp file on error TODO: move to tools
		if removeErr := os.Remove(tempOutputFilePath); removeErr != nil {
			fmt.Printf("Warning: failed to remove temp output file %s on error: %v\n", tempOutputFilePath, removeErr)
		}
		return fmt.Errorf("%s execution failed: %w", loopStepName, err) // Wrapped error
	}

	fmt.Printf("%s completed. Temporary loaded data at: %s\n", loopStepName, tempOutputFilePath) // Added success message back
	return nil
}

//goetl:type=load versioned_input=temp/MainLoop/loaded_records.jsonl
func LoadOutput() error {
	loadOutputStepName := "LoadOutput"
	mainLoopStepName := "MainLoop" // The step that produced the temporary input
	tempInputFileName := "loaded_records.jsonl"

	fmt.Printf("Starting %s...\n", loadOutputStepName)
	tempInputFilePath, _ := tools.GetTempFilePath(mainLoopStepName, tempInputFileName)

	finalOutputEncoder, finalOutputCloser, _, finalOutputFilePath, _ := tools.GetNextVersionedJSONLWriter(loadOutputStepName)
	defer finalOutputCloser()

	var recordsProcessed int64
	err := tools.StreamJSONLRecords(tempInputFilePath, TransformedUserData{}, func(record interface{}) error {
		transformedUser, _ := record.(*TransformedUserData)
		if err := finalOutputEncoder.Encode(transformedUser); err != nil {
			return fmt.Errorf("%s: failed to encode record %+v to final output %s: %w", loadOutputStepName, transformedUser, finalOutputFilePath, err)
		}
		recordsProcessed++
		return nil
	})

    // TODO: Handle recovery in tools
	if err != nil {
		// Attempt to remove partially written final output file on error
		if removeErr := os.Remove(finalOutputFilePath); removeErr != nil {
			fmt.Printf("%s: Warning - failed to remove partially written final output file %s on error: %v\n", loadOutputStepName, finalOutputFilePath, removeErr)
		}
		return fmt.Errorf("%s: failed to process records from %s: %w", loadOutputStepName, tempInputFilePath, err)
	}

	fmt.Printf("%s: Successfully processed %d records from %s to %s.\n", loadOutputStepName, recordsProcessed, tempInputFilePath, finalOutputFilePath)

	// 4. Optionally, clean up the temporary input file from MainLoop if everything was successful
	//    Be cautious with this if other processes might need the temp file or for debugging.
	//    If MainLoop might be re-run and produce a new temp file, deleting the old one is usually fine.
	if err := os.Remove(tempInputFilePath); err != nil {
		fmt.Printf("%s: Warning - failed to remove temporary input file %s: %v\n", loadOutputStepName, tempInputFilePath, err)
	} else {
		fmt.Printf("%s: Successfully removed temporary input file %s.\n", loadOutputStepName, tempInputFilePath)
	}

	return nil
}

func main() {
	tools.Knoll()

	finalStatusFile := "output/status/etl_run_status.json"

	run := tools.NewPipelineRun(finalStatusFile)
	run.LogStatus() // Log initial status

	run.ExecuteStep("ExtractUsers", ExtractUsers)
	run.ExecuteStep("MainLoop", MainLoop) // TODO: Add progress bar to tools
	run.ExecuteStep("LoadOutput", LoadOutput) // Ensure LoadOutput is called

	run.Stow() // This will save status if successful
}
