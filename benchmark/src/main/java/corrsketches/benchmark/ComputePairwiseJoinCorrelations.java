package corrsketches.benchmark;

import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.datasource.DBSource;
import corrsketches.benchmark.datasource.DBSource.DBColumnCombination;
import corrsketches.benchmark.params.SketchParams;
import corrsketches.benchmark.utils.CliTool;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = ComputePairwiseJoinCorrelations.JOB_NAME,
    description = "Compute correlation after joins of each pair of categorical-numeric column")
public class ComputePairwiseJoinCorrelations extends CliTool implements Serializable {

  public static final String JOB_NAME = "ComputePairwiseJoinCorrelations";

  enum BenchmarkType {
    CORR_STATS,
    CORR_PERF,
    MI
  }

  static final long SEED = 9;

  @Option(
      names = "--input-path",
      required = true,
      description = "Folder containing key-value column store")
  String inputPath;

  @Option(names = "--output-path", required = true, description = "Output path for results file")
  String outputPath;

  @Option(
      names = "--sketch-params",
      required = true,
      description =
          "A comma-separated list of sketch parameters in the format SKETCH_TYPE:BUDGET_VALUE,SKETCH_TYPE:BUDGET_VALUE,...")
  String sketchParams = null;

  @Option(
      names = "--intra-dataset-combinations",
      description = "Whether to consider only intra-dataset column combinations")
  Boolean intraDatasetCombinations = false;

  @Option(names = "--bench", description = "The type of benchmark to run")
  BenchmarkType benchmarkType = BenchmarkType.CORR_STATS;

  @Option(
      names = "--max-combinations",
      description = "The maximum number of columns to consider for creating combinations.")
  private int maxSamples = 5000;

  @Option(names = "--total-tasks", description = "The total number of tasks to split the computation.")
  private int totalTasks = -1;

  @Option(
      names = "--task-id",
      description = "The id of this task, a number in the range [0, <total-tasks> - 1]")
  private int taskId = -1;

  @Option(
      names = "--cpu-cores",
      description = "Number of CPU core to use. Default is to use all cores available.")
  int cpuCores = -1;

  @Option(
      names = "--right-aggregations",
      description = "Aggregation functions for the RIGHT table separated by comma (,), or \"all\"")
  String rightAggregateFunctions = "FIRST";

  @Option(
      names = "--left-aggregations",
      description = "Aggregation functions for the LEFT table separated by comma (,), or \"all\"")
  String leftAggregateFunctions = "NONE";

  public static void main(String[] args) {
    CliTool.run(args, new ComputePairwiseJoinCorrelations());
  }

  @Override
  public void execute() throws Exception {
    if (totalTasks > 0 && (taskId < 0 || taskId >= totalTasks)) {
      System.out.printf("taskId=[%d] must be a number from 0 to %d (total-tasks)\n", totalTasks-1);
      System.exit(1);
    }

    List<SketchParams> sketchParamsList = SketchParams.parse(this.sketchParams);
    System.out.println("> SketchParams: " + this.sketchParams);

    final List<AggregateFunction> leftAggregations = parseAggregations(this.leftAggregateFunctions);
    System.out.println("> LEFT aggregate functions: " + leftAggregations);

    final List<AggregateFunction> rightAggregations =
        parseAggregations(this.rightAggregateFunctions);
    System.out.println("> RIGHT aggregate functions: " + rightAggregations);

    // Set up data source
    System.out.println("\n> Computing column statistics for all column combinations...");
    DBSource dbsource = new DBSource(inputPath);
    List<DBColumnCombination> combinations =
        dbsource.createColumnCombinations(intraDatasetCombinations, maxSamples);

    // Initialize the output filename
    String datasetName = Paths.get(inputPath).getFileName().toString();
    String filename;
    if (totalTasks <= 0) {
      filename =
          String.format(
              "%s_bench-type=%s_sketch-params=%s.csv",
              datasetName, benchmarkType.toString(), sketchParams.toLowerCase());
    } else {
      filename =
          String.format(
              "%s_bench-type=%s_sketch-params=%s_task-id=%d_total_tasks=%d.csv",
              datasetName,
              benchmarkType.toString(),
              sketchParams.toLowerCase(),
              taskId,
              totalTasks);
    }

    // Set up the benchmark type
    final Benchmark bench;
    if (benchmarkType == BenchmarkType.CORR_PERF) {
      bench = new CorrelationPerformanceBenchmark();
    } else if (benchmarkType == BenchmarkType.CORR_STATS) {
      bench = new CorrelationStatsBenchmark();
    } else if (benchmarkType == BenchmarkType.MI) {
      bench = new MutualInformationBenchmark();
    } else {
      throw new IllegalArgumentException("Invalid benchmark type: " + benchmarkType);
    }

    // Initialize CSV output file and start writing headers
    Files.createDirectories(Paths.get(outputPath));
    String outputFileName = Paths.get(outputPath, filename).toString();
    System.out.println("> Writing output to file: " + outputFileName);
    FileWriter resultsFile = new FileWriter(outputFileName);
    resultsFile.write(bench.csvHeader() + "\n");

    // If necessary, filter combinations leaving only the ones that should be computed by this task
    System.out.println("> Total number of column combinations: " + combinations.size());
    if (totalTasks > 1) {
      combinations =
          IntStream.range(0, combinations.size())
              .filter(i -> i % totalTasks == taskId)
              .mapToObj(combinations::get)
              .collect(Collectors.toList());
      Collections.shuffle(combinations, new Random(SEED));
      System.out.println("> Column combinations for this task: " + combinations.size());
    }

    final Stream<DBColumnCombination> stream = combinations.stream();
    final int total = combinations.size();
    final AtomicInteger processed = new AtomicInteger(0);
    final int cores = cpuCores > 0 ? cpuCores : Runtime.getRuntime().availableProcessors();

    ForkJoinPool forkJoinPool = new ForkJoinPool(cores);
    Runnable task =
        () ->
            stream
                .parallel()
                .map(
                    (DBColumnCombination columnPair) -> {
                      List<String> results =
                          bench.run(
                              columnPair, sketchParamsList, leftAggregations, rightAggregations);
                      reportProgress(processed, total);
                      return toCSV(results);
                    })
                .forEach(writeCSV(resultsFile));
    forkJoinPool.submit(task).get();

    resultsFile.close();
    dbsource.close();

    System.out.println(getClass().getSimpleName() + " finished successfully.");
  }

  static List<AggregateFunction> parseAggregations(String functions) {
    if (functions == null || functions.isEmpty()) {
      System.out.println("No aggregate functions configured. Using default: FIRST");
      return Collections.singletonList(AggregateFunction.FIRST);
    }
    if (functions.equals("all")) {
      return AggregateFunction.all();
    }
    final List<AggregateFunction> aggregateFunctions = new ArrayList<>();
    for (String functionName : functions.split(",")) {
      try {
        aggregateFunctions.add(AggregateFunction.valueOf(functionName));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Unrecognized aggregate functions name: " + functionName, e);
      }
    }
    return aggregateFunctions;
  }

  static void reportProgress(AtomicInteger processed, int total) {
    int current = processed.incrementAndGet();
    if (current % 1000 == 0) {
      double percent = 100 * current / (double) total;
      synchronized (System.out) {
        System.out.printf("Progress: %.3f%%\n", percent);
      }
    }
  }

  protected static String toCSV(List<String> results) {
    if (results == null || results.isEmpty()) {
      return "";
    } else {
      StringBuilder builder = new StringBuilder();
      for (String result : results) {
        builder.append(result);
      }
      return builder.toString();
    }
  }

  protected static Consumer<String> writeCSV(FileWriter file) {
    return (String line) -> {
      if (!line.isEmpty()) {
        synchronized (file) {
          try {
            file.write(line);
            file.flush();
          } catch (IOException e) {
            throw new RuntimeException("Failed to write line to file: " + line);
          }
        }
      }
    };
  }
}
