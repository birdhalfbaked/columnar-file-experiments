package main

import (
	columnarfiles "columnarfiles/pkg"
	"fmt"
	"os"
	"strconv"
	"time"
)

type ExampleStruct struct {
	ColumnInt    int64
	ColumnFloat  float32
	ColumnFloat2 float64
	ColumnString string
	ColumnBool   bool
}

func benchmarkNaive(samples, numSplits, numCols int) {
	fmt.Println("File type: Naive Columnar File")
	cols := []string{"ColumnInt", "ColumnFloat", "ColumnFloat2", "ColumnString", "ColumnBool"}
	processor := columnarfiles.NewParallelReadProcessor("./data/output/naive_test.naive", numSplits)
	totalTime := 0.0
	for i := 0; i < samples; i++ {
		startTime := time.Now()
		for _, record := range processor.Scan(cols[:numCols]...) {
			resultContainer := ExampleStruct{}
			record.Scan([]any{&resultContainer.ColumnInt, &resultContainer.ColumnFloat, &resultContainer.ColumnFloat2, &resultContainer.ColumnString, &resultContainer.ColumnBool}[:numCols])
		}
		samples--
		totalTime += time.Since(startTime).Seconds()
	}
	fmt.Println("\taverage time:", totalTime/float64(samples))
	fmt.Println("\ttotal time:", totalTime)
}

func benchmarkJSONNewLine(samples, numSplits, numCols int) {
	fmt.Println("File type: JSON newline File")
	cols := []string{"column_int", "column_float", "column_float_2", "column_string", "column_bool"}
	processor := columnarfiles.NewParallelReadProcessor("./data/dummy/to_load.jsonl", numSplits)
	totalTime := 0.0
	for i := 0; i < samples; i++ {
		startTime := time.Now()
		for _, record := range processor.Scan(cols[:numCols]...) {
			resultContainer := ExampleStruct{}
			record.Scan([]any{&resultContainer.ColumnInt, &resultContainer.ColumnFloat, &resultContainer.ColumnFloat2, &resultContainer.ColumnString, &resultContainer.ColumnBool}[:numCols]...)
			// fmt.Println(resultContainer)
		}
		samples--
		totalTime += time.Since(startTime).Seconds()
	}
	fmt.Println("\taverage time:", totalTime/float64(samples))
	fmt.Println("\ttotal time:", totalTime)

}

func main() {
	parallelProcesses, _ := strconv.Atoi(os.Args[1])
	numColumns, _ := strconv.Atoi(os.Args[2])
	numSamples := 10
	fmt.Println("Parallel processors:", parallelProcesses)
	fmt.Println("Selected Columns:", numColumns)
	benchmarkJSONNewLine(numSamples, parallelProcesses, numColumns) // the baseline benchmark :D
	benchmarkNaive(numSamples, parallelProcesses, numColumns)
}
