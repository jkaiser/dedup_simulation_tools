# dedup_simulation_tools

## parser
Various parsing libraries for different deduplication trace formats. 

**ubc** : The format as used by Meyer et. al [1].

**proto**: The format as used by Dirk Meister and which is generated by his fs-c tool (https://github.com/dmeister/fs-c).

**legacy**: An trace format used in Meister's early research.

## traceProto
The protocol buffer files used for the proto traces.

## generator
Takes the deduplication traces as collected by Dutch Meyer et. al [1] and converts them into the traces as the fs-c tool from Dirk Meister would create them (github.com/dmeister/fs-c). The latter is the input format for the deduplication simulator.

The conversion uses metadata from Meyer's files. These are given in the all_file_metadata.txt file.

## chunk_skewness
Small tools to compute the chunk skewness, i.e. how many chunks occur how many times in a given trace.
NOTE: The tool depends on the deduplication simulator.


[1] A study of practical deduplication, DT Meyer, WJ Bolosky - ACM Transactions on Storage (TOS), 2012
