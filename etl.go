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
	
	encoder := tools.GetCurrentEncoder()
	filePath := tools.GetCurrentFilePath()
	
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
	fmt.Printf("Users extracted successfully to %s\n", filePath)
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
	err := tools.ProcessStreamedRecordsSimplified(
		UserData{},
		transformSingleRecord,
		loadSingleRecord,
	)
	return err
}

//goetl:type=load versioned_input=temp/MainLoop/loaded_records.jsonl
func LoadOutput() error {
	loadOutputStepName := "LoadOutput"
	mainLoopStepName := "MainLoop"
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

	if err != nil {
		// Attempt to remove partially written final output file on error
		if removeErr := os.Remove(finalOutputFilePath); removeErr != nil {
			fmt.Printf("%s: Warning - failed to remove partially written final output file %s on error: %v\n", loadOutputStepName, finalOutputFilePath, removeErr)
		}
		return fmt.Errorf("%s: failed to process records from %s: %w", loadOutputStepName, tempInputFilePath, err)
	}

	fmt.Printf("%s: Successfully processed %d records from %s to %s.\n", loadOutputStepName, recordsProcessed, tempInputFilePath, finalOutputFilePath)

	// Clean up the temporary input file
	if err := os.Remove(tempInputFilePath); err != nil {
		fmt.Printf("%s: Warning - failed to remove temporary input file %s: %v\n", loadOutputStepName, tempInputFilePath, err)
	} else {
		fmt.Printf("%s: Successfully removed temporary input file %s.\n", loadOutputStepName, tempInputFilePath)
	}

	return nil
}

func main() {
	tools.Knoll()
	run := tools.NewPipelineRun("output/status/etl_run_status.json")
	run.LogStatus()
	run.ExecuteStep("ExtractUsers", ExtractUsers)
	run.ExecuteStep("MainLoop", MainLoop)
	run.ExecuteStep("LoadOutput", LoadOutput)
	run.Stow() // This will save status if successful
}
