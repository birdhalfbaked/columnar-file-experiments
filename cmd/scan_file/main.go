package main

import (
	columnarfiles "columnarfiles/pkg"
	"fmt"
	"time"
)

type ExampleStruct struct {
	ColumnInt    int64
	ColumnFloat  float32
	ColumnFloat2 float64
	ColumnString string
	ColumnBool   bool
}

func benchmarkNaive(samples, numSplits int) {
	fmt.Println("File type: Naive Columnar File")
	processor := columnarfiles.NewParallelReadProcessor("./data/output/naive_test.naive", numSplits)
	totalTime := 0.0
	for i := 0; i < samples; i++ {
		startTime := time.Now()
		for _, record := range processor.Scan() {
			resultContainer := ExampleStruct{}
			record.Scan(&resultContainer.ColumnInt, &resultContainer.ColumnFloat, &resultContainer.ColumnFloat2, &resultContainer.ColumnString, &resultContainer.ColumnBool)
		}
		samples--
		totalTime += time.Since(startTime).Seconds()
	}
	fmt.Println("\taverage time:", totalTime/float64(samples))
	fmt.Println("\ttotal time:", totalTime)
}

func benchmarkJSONNewLine(samples, numSplits int) {
	fmt.Println("File type: JSON newline File")
	processor := columnarfiles.NewParallelReadProcessor("./data/dummy/to_load.jsonl", numSplits)
	totalTime := 0.0
	for i := 0; i < samples; i++ {
		startTime := time.Now()
		for _, record := range processor.Scan() {
			resultContainer := ExampleStruct{}
			record.Scan(&resultContainer.ColumnInt, &resultContainer.ColumnFloat, &resultContainer.ColumnFloat2, &resultContainer.ColumnString, &resultContainer.ColumnBool)
		}
		samples--
		totalTime += time.Since(startTime).Seconds()
	}
	fmt.Println("\taverage time:", totalTime/float64(samples))
	fmt.Println("\ttotal time:", totalTime)

}

func main() {
	numSamples := 10
	for numSplits := 1; numSplits < 6; numSplits++ {
		fmt.Println("Split number:", numSplits)
		benchmarkJSONNewLine(numSamples, numSplits) // the benchmark :D
		benchmarkNaive(numSamples, numSplits)
	}
}
