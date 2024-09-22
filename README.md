# columnar-file-experiments
Me messing around with some stuff. Let the dragons loose!

## but WHY?

Because I feel like it and I also was needing something to help teach some columnar formats to my team at work :D. It is pretty fun, and there's a lot of interesting ideas
that you naturally come to when seeing implementations and the types of operations that
happen on data files. Benchmarks also include some things like jsonl (and I probably should do CSV since that can actually be column-parallelized with benefit)

## How to use this repo

In order to not cheat as much as possible since this will eventually cover write benchmarks
when I get to it, I don't include anything but the dummy data. To be honest though, you
could manipulate this to make it work with ANY data (though with the jsonnl reader you would need to adjust the processor columns I hardcoded. Sorry)

To run the existing benchmarks:
1. `go run cmd/create_file/main.go`
    * this creates the custom file formats we want
2. `go run cmd/scan_file/main.go [parallelReads] [numColumns]`
    * e.g. `go run cmd/scan_file/main.go 2 3`
    * this runs the benchmark by going over a number of split processes (technically go-routines... but hey, the scheduler is pretty smart in go)

Running the program `go run cmd/scan_file/main.go 3 3` will scan the file data, and will
attempt to parallelize over 3 routines to select 3 columns. This in practice means that in
the columnar format, each routine reads one column. This shows the power of columnar data
stores with the speed quite readily even in the naive case.

```
Parallel processors: 3
Selected Columns: 3
File type: JSON newline File
        average time: 0.59700844
        total time: 2.9850422
File type: Naive Columnar File
        average time: 0.26409530000000003
        total time: 1.3204765
```

## What formats are there implemented so far?

I add comments to the subpackages (mostly to help me remember what
 I specced while I code in a split window environment :P).

However, here is the list:

### Naive Columnar Store
Naive files will just be a single-pass write solution with the following format
spec
```
+------------------------------------------------------+
|                                                      |
|                         DATA                         |
|                                                      |
+------------------------------------------------------+
|                       Metadata                       |
| Row count (uint64)                                   |
| Column count (uint64)                                |
| Columns [64 byte name, 4 byte type, 8 byte offset]   |
| Metadata length uint32                               |
+------------------------------------------------------+
```

This is not efficiently packed, nor is it optimized for compression,
etc. but it gets the job done.

It also has one massive limitation: can't really do nested structures.
Well... technically you can, but nested structure striping is not imo a naive solution.
For now we can just do the silly thing.

#### Example with the test data

The test data when stored in this format looks like the following:
**Data**
```
+------------------------------------------------------+
|                       Data                           |
|     [    ColumnInt            Value 1   ]            |
|     [    ColumnInt            Value 2   ]            |
|     [                  ...              ]            |
|     [    ColumnInt            Value N   ]            |
|     [    ColumnFloat          Value 1   ]            |
|     [    ColumnFloat          Value 2   ]            |
|     [                  ...              ]            |
|     [    ColumnFloat          Value N   ]            |
|     [    ColumnFloat2         Value 1   ]            |
|     [    ColumnFloat2         Value 2   ]            |
|     [                  ...              ]            |
|     [    ColumnFloat2         Value N   ]            |
|     [    ColumnString         Value 1   ]            |
|     [    ColumnString         Value 2   ]            |
|     [                  ...              ]            |
|     [    ColumnString         Value N   ]            |
|     [    ColumnBool           Value 1   ]            |
|     [    ColumnBool           Value 2   ]            |
|     [                  ...              ]            |
|     [    ColumnBool           Value N   ]            |
+------------------------------------------------------+
```
**Metadata**
```
+------------------------------------------------------+
|                       Metadata                       |
| Row count:    100000                                 |
| Column count: 5                                      |
| Columns:                                             |
|   - "ColumnInt" (padded to 64 bytes)                 |
|     Int64                                            |
|     [first value file offset]                        |
|   - "ColumnFloat" (padded to 64 bytes)               |
|     Float64                                          |
|     [first value file offset]                        |
|   - "ColumnFloat2" (padded to 64 bytes)              |
|     Float32                                          |
|     [first value file offset]                        |
|   - "ColumnString" (padded to 64 bytes)              |
|     String                                           |
|     [first value file offset]                        |
|   - "ColumnBool" (padded to 64 bytes)                |
|     Bool                                             |
|     [first value file offset]                        |
| Metadata length uint32                               |
+------------------------------------------------------+
```
