package columnarfiles

import (
	"bufio"
	"columnarfiles/pkg/naive"
	"columnarfiles/pkg/types"
	"encoding/json"
	"log/slog"
	"os"
	"path"
)

type FileType string

const (
	JSONNewLineStore FileType = ".jsonl"
	NaiveFileStore   FileType = ".naive"
)

type ParallelReadResult struct {
	columns []string
	data    [][]any
}

type ParallelReadProcessor struct {
	resultPipe    chan *ParallelReadResult
	filePath      string
	parallelReads int
}

func NewParallelReadProcessor(filePath string, parallelReads int) *ParallelReadProcessor {
	return &ParallelReadProcessor{
		resultPipe:    make(chan *ParallelReadResult),
		filePath:      filePath,
		parallelReads: parallelReads,
	}
}

func stringListContainsItem(arr []string, value string) bool {
	for _, v := range arr {
		if v == value {
			return true
		}
	}
	return false
}

func (p *ParallelReadProcessor) scanJSONNewLineFile(cols ...string) []*types.Record {
	file, err := os.Open(p.filePath)
	if err != nil {
		slog.Error(err.Error())
		return nil
	}
	// Parallel Processing is possible at row level
	// but it doesn't actually matter because it would be more costly to
	// have to scan through and check how many newlines there are.
	// For this reason, we will ignore ther parallel processing config
	resultsArr := make([]*types.Record, 0)
	scanner := bufio.NewScanner(file)
	// fudge column order because I can't be bothered to implement this properly
	columns := []string{
		"column_int",
		"column_float",
		"column_float_2",
		"column_string",
		"column_bool",
	}
	for scanner.Scan() {
		data := make(map[string]interface{})
		json.Unmarshal(scanner.Bytes(), &data)
		recData := make([]any, len(columns))
		for i, col := range columns {
			// also hack around just getting json interface to float32 and int64 without
			// having to properly deserialize for some slight advantage to jsonl
			if !stringListContainsItem(cols, col) {
				continue
			}
			if i == 0 {
				recData[i] = int64(data[col].(float64))
			} else if i == 1 {
				recData[i] = float32(data[col].(float64))
			} else {
				recData[i] = data[col]
			}
		}
		resultsArr = append(resultsArr, &types.Record{Data: recData})
	}
	return resultsArr
}

func (p *ParallelReadProcessor) scanNaiveFile(cols ...string) []*types.Record {
	file, err := naive.Open(p.filePath)
	if err != nil {
		slog.Error(err.Error())
		return nil
	}
	colCount := file.Metadata.ColumnCount
	resultsArr := make([]*types.Record, file.Metadata.RowCount)
	for i := 0; i < len(resultsArr); i++ {
		resultsArr[i] = &types.Record{Data: make([]any, colCount)}
	}
	columnSplits := make([][]string, p.parallelReads)
	nameToIndex := make(map[string]int)
	colsReadied := 0
	for i := 0; i < int(colCount); i++ {
		col := file.Metadata.Columns[i]
		nameToIndex[col.Name()] = i
		if !stringListContainsItem(cols, col.Name()) {
			continue
		}
		idx := colsReadied % p.parallelReads
		if columnSplits[idx] == nil {
			columnSplits[idx] = make([]string, 0)
		}
		columnSplits[idx] = append(columnSplits[idx], col.Name())
		colsReadied++
	}
	for i := 0; i < p.parallelReads; i++ {
		go p.scanSegment(NaiveFileStore, columnSplits[i]...)
	}
	for i := 0; i < p.parallelReads; i++ {
		result := <-p.resultPipe
		for idx, colName := range result.columns {
			colIdx := nameToIndex[colName]
			for r := 0; r < len(resultsArr); r++ {
				resultsArr[r].Data[colIdx] = result.data[idx][r]
			}
		}
	}
	return resultsArr
}

func (p *ParallelReadProcessor) Scan(cols ...string) []*types.Record {
	// Full read into mem since this is all just for fun. In reality
	// you would want to chunk this, but this gets into a fun topic I will
	// sketch up later which goes into row groupings and column chunks
	switch t := FileType(path.Ext(p.filePath)); t {
	case NaiveFileStore:
		return p.scanNaiveFile(cols...)
	case JSONNewLineStore:
		return p.scanJSONNewLineFile(cols...)
	}
	return nil
}

func (p *ParallelReadProcessor) scanSegment(_type FileType, columns ...string) {
	// We will open a new file. Normally you'd also want some sort of
	// internal mutex to lock things in the case something tries to simultaneously
	// overwrite... but since this is all toy examples, it's fiiiine
	var file ColumnarStore
	defer func() {
		if file != nil {
			file.Close()
		}
	}()
	switch _type {
	case NaiveFileStore:
		file, _ = naive.Open(p.filePath)
	}
	result := &ParallelReadResult{
		columns: columns,
		data:    file.Scan(columns...),
	}
	p.resultPipe <- result
}
