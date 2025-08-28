package main

import (
	"encoding/json"
	"fmt"
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
	loadOutputStepName := tools.GetStepName()
	tempInputFilePath := tools.GetTempInputFilePath()
	
	fmt.Printf("Starting %s...\n", loadOutputStepName)

	finalOutputEncoder, finalOutputCloser, _, finalOutputFilePath, _ := tools.GetNextVersionedJSONLWriter(loadOutputStepName)
	defer finalOutputCloser()
	
	// Set the final output path in context for cleanup
	tools.SetFinalOutputPath(finalOutputFilePath)
	
	err := tools.StreamJSONLRecords(tempInputFilePath, TransformedUserData{}, func(record interface{}) error {
		transformedUser, _ := record.(*TransformedUserData)
		if err := finalOutputEncoder.Encode(transformedUser); err != nil {
			return fmt.Errorf("%s: failed to encode record %+v to final output %s: %w", loadOutputStepName, transformedUser, finalOutputFilePath, err)
		}
		tools.IncrementRecordsProcessed()
		return nil
	})

	// Handle cleanup using simplified context-based function
	return tools.HandleFileCleanupAfterProcessingSimplified(err)
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
