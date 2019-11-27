INPUT_PATH=${INPUT_PATH:-"./notebooks/synthetic-correlated-joinable-small/"}
STORE_PATH=${STORE_PATH:-"./notebooks/tmp/db/synthetic-correlated-joinable-small/"}
RESULTS_PATH=${RESULTS_PATH:-"./notebooks/results/tmp/results/"}
SKETCH_TYPE=${SKETCH_TYPE:-"KMV"}
BUDGET=${BUDGET:-256}
DBTYPE=${DBTYPE:-"ROCKSDB"}

JAR=benchmark/build/libs/benchmark-0.1-SNAPSHOT-all.jar
BENCHMARK_EXE="java -cp $JAR benchmark.ComputePairwiseCorrelationJoinsThreads"
CREATE_STORE_EXE="java -cp $JAR benchmark.CreateColumnStore"

CREATE_STORE_CMD="$CREATE_STORE_EXE --input-path $INPUT_PATH --output-path $STORE_PATH --db-backend $DBTYPE"
BENCHMARK_CMD="$BENCHMARK_EXE --input-path $STORE_PATH --output-path $RESULTS_PATH --sketch-type $SKETCH_TYPE --num-hashes $BUDGET"

echo "Compiling application..."
gradle shadowJar

echo "Create Column Key-Value Store..."
echo "Command: $CREATE_STORE_CMD"
$CREATE_STORE_CMD

echo "Running benchmark..."
echo "Command: $BENCHMARK_CMD"
$BENCHMARK_CMD
