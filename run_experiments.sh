#
# Runs experiments for fixed datasets and parameters
#
# Example:
#
#  DB=LEVELDB COMPILE=false JAR=./benchmark-0.1-SNAPSHOT-all.jar BASE_OUTPUT_PATH=./output DATASETS_PATH="./datasets" ./run_experiments.sh
#

# Default parameters
DB=${DB:-"ROCKSDB"}
COMPILE=${COMPILE:-"true"}
BASE_OUTPUT_PATH=${BASE_OUTPUT_PATH:-"./notebooks/results"}
DATASETS_PATH=${DATASETS_PATH:-"./datasets"}
JAR=${JAR:-"benchmark/build/libs/benchmark-0.1-SNAPSHOT-all.jar"}

BENCHMARK_EXE="java -cp $JAR benchmark.ComputePairwiseCorrelationJoinsThreads"
CREATE_STORE_EXE="java -cp $JAR benchmark.CreateColumnStore"

create_store () {
  local INPUT_PATH=$1
  local STORE_PATH=$2
  local CREATE_STORE_CMD="$CREATE_STORE_EXE --input-path $INPUT_PATH --output-path $STORE_PATH --db-backend $DB"
  echo "Creating store for $INPUT_PATH"
  $CREATE_STORE_CMD
}

run_kmv () {
  local STORE_PATH=$1
  local RESULTS_PATH=$2
  local K_VALUES=$3
  for k in $K_VALUES; do
    local CMD="$BENCHMARK_EXE --input-path $STORE_PATH --output-path $RESULTS_PATH --sketch-type KMV --num-hashes $k"
    echo "Running command: $CMD"
    $CMD
  done
}

run_gkmv () {
  local STORE_PATH=$1
  local RESULTS_PATH=$2
  local TAU=$3
  for T in $TAU; do
    local CMD="$BENCHMARK_EXE --input-path $STORE_PATH --output-path $RESULTS_PATH --sketch-type GKMV --num-hashes $T"
    echo "Running command: $CMD"
    $CMD
  done
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
#TAU="0.003"
#K_VALUES="128"
#
#  INPUT_PATH="$DATASETS_PATH/$DATASET_NAME"
#  STORE_PATH="$BASE_OUTPUT_PATH/db/$DATASET_NAME"
#RESULTS_PATH="$BASE_OUTPUT_PATH/results/$DATASET_NAME"
#
#create_store "$INPUT_PATH" "$STORE_PATH"
#run_kmv  "$STORE_PATH" "$RESULTS_PATH" "$K_VALUES"
#run_gkmv "$STORE_PATH" "$RESULTS_PATH" "$TAU"


#
#  Parameter equivalency for each method in the large synthetic dataset
#
#  k=128 budget=128*500=64000 tau=0.003 tau-unique=0.012
#  k=256 budget=256*500=128000 tau=0.007 tau-unique=0.023
#  k=512 budget=512*500=256000 tau=0.013 tau-unique=0.047
#  k=1024 budget=1024*500=512000 tau=0.027 tau-unique=0.093
#

DATASET_NAME="synthetic-correlated-joinable-large"
TAU="0.003 0.007 0.013 0.027"
K_VALUES="128 256 512 1024"

  INPUT_PATH="$DATASETS_PATH/$DATASET_NAME"
  STORE_PATH="$BASE_OUTPUT_PATH/db/$DATASET_NAME"
RESULTS_PATH="$BASE_OUTPUT_PATH/results/$DATASET_NAME"

#create_store "$INPUT_PATH" "$STORE_PATH"
run_kmv  "$STORE_PATH" "$RESULTS_PATH" "$K_VALUES"
run_gkmv "$STORE_PATH" "$RESULTS_PATH" "$TAU"

#
#  Parameter equivalency for each method in the Worldbank Finances dataset
#
#  k=128 budget=128*521=66688 tau=0.110 tau-unique=0.134
#  k=256 budget=256*521=133376 tau=0.221 tau-unique=0.268
#  k=512 budget=512*521=266752 tau=0.442 tau-unique=0.535
#  k=1024 budget=1024*521=533504 tau=0.883 tau-unique=1.070
#

DATASET_NAME="finances.worldbank.org"
TAU="0.110 0.221 0.442 0.883"
K_VALUES="128 256 512 1024"

  INPUT_PATH="$DATASETS_PATH/$DATASET_NAME"
  STORE_PATH="$BASE_OUTPUT_PATH/db/$DATASET_NAME"
RESULTS_PATH="$BASE_OUTPUT_PATH/results/$DATASET_NAME"

create_store "$INPUT_PATH" "$STORE_PATH"
run_kmv  "$STORE_PATH" "$RESULTS_PATH" "$K_VALUES"
run_gkmv "$STORE_PATH" "$RESULTS_PATH" "$TAU"
