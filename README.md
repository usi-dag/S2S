# S2S (SQL to Stream) - Benchmark Suite Generator for the Java Stream API

S2S is a benchmark suite generator which compiles SQL queries into Java source code which makes use of the Stream API.

More info on the paper:

SQL to Stream with S2S - An Automatic Benchmark Generator for the Java Stream API

https://doi.org/10.1145/3564719.3568699

## Build

Run `mvn clean package`

## Usage Example

### Run the example and build BSS

Use the script `gen.sh`

### Example input data

To generate a dataset for BSS with different scale factor, e.g., SF 1:

SCALE_FACTOR=1 ./get-tpch-db.sh