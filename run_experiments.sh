#
# Runs experiments for fixed datasets and parameters
#
# Example:
#
#  DB=LEVELDB COMPILE=false JVM_ARGS="-Xmx32768m -Xms32768m" JAR=./benchmark-0.1-SNAPSHOT-all.jar BASE_OUTPUT_PATH=./output DATASETS_PATH="./datasets" ./run_experiments.sh
#

# Default parameters
DB=${DB:-"ROCKSDB"}
COMPILE=${COMPILE:-"true"}
BASE_OUTPUT_PATH=${BASE_OUTPUT_PATH:-"./notebooks/results"}
DATASETS_PATH=${DATASETS_PATH:-"./datasets"}
JAR=${JAR:-"benchmark/build/libs/benchmark-0.1-SNAPSHOT-all.jar"}
JVM_ARGS=${JVM_ARGS:-""}
PERFORMANCE=${PERFORMANCE:-"false"}

BENCHMARK_EXE="java $JVM_ARGS -cp $JAR benchmark.ComputePairwiseCorrelationJoinsThreads"
CREATE_STORE_EXE="java $JVM_ARGS -cp $JAR benchmark.CreateColumnStore"

create_store () {
  local DATASETS_PATH=$1
  local DATASET_NAME=$2

  local INPUT_PATH="$DATASETS_PATH/$DATASET_NAME"
  local STORE_PATH="$BASE_OUTPUT_PATH/db/$DATASET_NAME"

  local CREATE_STORE_CMD="$CREATE_STORE_EXE --input-path $INPUT_PATH --output-path $STORE_PATH --db-backend $DB"

  echo "Creating store for $INPUT_PATH"
  echo "Running command: $CREATE_STORE_CMD"
  $CREATE_STORE_CMD
}

run_benchmark () {
  local DATASET_NAME="$1"
  local SKETCH_PARAMS="$2"

  local STORE_PATH="$BASE_OUTPUT_PATH/db/$DATASET_NAME"
  local RESULTS_PATH="$BASE_OUTPUT_PATH/results/$DATASET_NAME"

  if [ "$PERFORMANCE" = "true" ]
  then
    PERF_ARG="--performance"
  fi

  local CMD="$BENCHMARK_EXE --input-path $STORE_PATH --output-path $RESULTS_PATH --sketch-params $SKETCH_PARAMS $PERF_ARG"
  echo "Running command: $CMD"
  $CMD

}

if [ "$COMPILE" = "true" ]
then
  echo "Compiling application..."
  gradle shadowJar
fi

#
# Test script on small synthetic data
#

#DATASET_NAME="synthetic-correlated-joinable-small"
#SKETCH_PARAMS="KMV:128,GKMV:0.003"

#
#  Parameter equivalency for each method in the large synthetic dataset
#
#  k=128 budget=128*500=64000 tau=0.003 tau-unique=0.012
#  k=256 budget=256*500=128000 tau=0.007 tau-unique=0.023
#  k=512 budget=512*500=256000 tau=0.013 tau-unique=0.047
#  k=1024 budget=1024*500=512000 tau=0.027 tau-unique=0.093
#

#DATASET_NAME="synthetic-correlated-joinable-large"
#SKETCH_PARAMS="KMV:128,GKMV:0.003,KMV:256,GKMV:0.007,KMV:512,GKMV:0.013,KMV:1024,GKMV:0.027"

#
#  Parameter equivalency for each method in the Worldbank Finances dataset
#
#  k=128 budget=128*521=66688 tau=0.110 tau-unique=0.134
#  k=256 budget=256*521=133376 tau=0.221 tau-unique=0.268
#  k=512 budget=512*521=266752 tau=0.442 tau-unique=0.535
#  k=1024 budget=1024*521=533504 tau=0.883 tau-unique=1.070
#

DATASET_NAME="finances.worldbank.org"
SKETCH_PARAMS="KMV:128,GKMV:0.110,KMV:256,GKMV:0.221,KMV:512,GKMV:0.442,KMV:1024,GKMV:0.883"

#
#  Parameter equivalency for data.cityofnewyork.us
#
#  k=128 budget=128*15842=2027776 unique-keys=46655726 unique-keys=46655726 tau=0.04346
#  k=256 budget=256*15842=4055552 unique-keys=46655726 unique-keys=46655726 tau=0.08693
#  k=512 budget=512*15842=8111104 unique-keys=46655726 unique-keys=46655726 tau=0.17385
#  k=1024 budget=1024*15842=16222208 unique-keys=46655726 unique-keys=46655726 tau=0.34770
#

#DATASET_NAME="data.cityofnewyork.us"
#SKETCH_PARAMS="KMV:128,GKMV:0.04346,KMV:256,GKMV:0.08693,KMV:512,GKMV:0.17385,KMV:1024,GKMV:0.34770"

create_store "$DATASETS_PATH" "$DATASET_NAME"
run_benchmark "$DATASET_NAME" "$SKETCH_PARAMS"