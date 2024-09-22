package main

import (
	"bufio"
	"columnarfiles/pkg/naive"
	"encoding/json"
	"os"
)

type ExampleStruct struct {
	ColumnInt    int64   `json:"column_int"`
	ColumnFloat  float32 `json:"column_float"`
	ColumnFloat2 float64 `json:"column_float_2"`
	ColumnString string  `json:"column_string"`
	ColumnBool   bool    `json:"column_bool"`
}

func loadDummyData() []interface{} {
	dataArr := make([]interface{}, 0)
	file, _ := os.Open("data/dummy/to_load.jsonl")
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var data ExampleStruct
		json.Unmarshal(scanner.Bytes(), &data)
		dataArr = append(dataArr, data)
	}
	return dataArr
}

func main() {
	data := loadDummyData()
	file, _ := naive.Open("./data/output/naive_test.naive")
	file.Write(data)
}
