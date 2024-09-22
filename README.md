# columnar-file-experiments
Me messing around with some stuff. Let the dragons loose!

## but WHY?

Because I feel like it and I also was needing something to help teach some columnar formats to my team at work :D. It is pretty fun, and there's a lot of interesting ideas
that you naturally come to when seeing implementations and the types of operations that
happen on data files.

## How to use this repo

In order to not cheat as much as possible since this will eventually cover write benchmarks
when I get to it, I don't include anything but the dummy data. To be honest though, you
could manipulate this to make it work with ANY data (though with the jsonnl reader you would need to adjust the processor columns I hardcoded. Sorry)

To run the existing benchmarks:
1. `go run cmd/create_file/main.go`
    * this creates the custom file formats we want
2. `go run cmd/scan_file/main.go`
    * this runs the benchmark by going over a number of split processes (technically go-routines... but hey, the scheduler is pretty smart in go)

This will output the following example info:

```
Split number: 1
File type: JSON newline File
        average time: 0.50405924
        total time: 2.5202962
File type: Naive Columnar File
        average time: 0.86312088
        total time: 4.3156044
Split number: 2
File type: JSON newline File
        average time: 0.49922840000000007
        total time: 2.4961420000000003
File type: Naive Columnar File
        average time: 0.5937488
        total time: 2.968744
Split number: 3
File type: JSON newline File
        average time: 0.49667472
        total time: 2.4833736
File type: Naive Columnar File
        average time: 0.57459428
        total time: 2.8729714
Split number: 4
File type: JSON newline File
        average time: 0.49486613999999995
        total time: 2.4743307
File type: Naive Columnar File
        average time: 0.47310418
        total time: 2.3655209
Split number: 5
File type: JSON newline File
        average time: 0.4958919
        total time: 2.4794595
File type: Naive Columnar File
        average time: 0.45844801999999996
        total time: 2.2922401
```

## What formats are there implemented so far?

I add comments to the subpackages (mostly to help me remember what
 I specced while I code in a split window environment :P).

However, here is the list:

### Naive Columnar Store
Naive files will just be a single-pass write solution with the following format
spec
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

This is not efficiently packed, nor is it optimized for compression,
etc. but it gets the job done.

It also has one massive limitation: can't really do nested structures.
Well... technically you can, but nested structure striping is not imo a naive solution.
For now we can just do the silly thing.
