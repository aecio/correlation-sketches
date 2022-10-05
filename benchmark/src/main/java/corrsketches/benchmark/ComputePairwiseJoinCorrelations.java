package corrsketches.benchmark;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.CreateColumnStore.ColumnStoreMetadata;
import corrsketches.benchmark.params.SketchParams;
import corrsketches.benchmark.utils.CliTool;
import edu.nyu.engineering.vida.kvdb4j.api.StringObjectKVDB;
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

  @Option(names = "--total-tasks", description = "The to number of tasks to split the computation.")
  private int totalTasks = -1;

  @Option(
      names = "--task-id",
      description = "The id of this task, a number in the range [0, <total-tasks> - 1]")
  private int taskId = -1;

  @Option(
      names = "--cpu-cores",
      description = "Number of CPU core to use. Default is to use all cores available.")
  int cpuCores = -1;

  @Option(names = "--aggregations", description = "Run performance experiments")
  String aggregateFunctions = "FIRST";

  public static void main(String[] args) {
    CliTool.run(args, new ComputePairwiseJoinCorrelations());
  }

  @Override
  public void execute() throws Exception {
    if (totalTasks > 0 && taskId < 0) {
      System.out.printf("taskId=[%d] must be a number from 0 to %d (total-tasks)\n", totalTasks);
      System.exit(1);
    }

    List<SketchParams> sketchParamsList = SketchParams.parse(this.sketchParams);
    System.out.println("> SketchParams: " + this.sketchParams);

    List<AggregateFunction> aggregations = parseAggregations(this.aggregateFunctions);
    System.out.println("> Using aggregate functions: " + aggregations);

    ColumnStoreMetadata storeMetadata = CreateColumnStore.readMetadata(inputPath);

    final boolean readonly = true;
    StringObjectKVDB<ColumnPair> columnStore =
        CreateColumnStore.KVColumnStore.create(inputPath, storeMetadata.dbType, readonly);

    Set<Set<String>> columnSets = storeMetadata.columnSets;
    System.out.println(
        "> Found  " + columnSets.size() + " column pair sets in DB stored at " + inputPath);

    System.out.println("\n> Computing column statistics for all column combinations...");
    List<ColumnCombination> combinations =
        ColumnCombination.createColumnCombinations(
            columnSets, intraDatasetCombinations, maxSamples);

    String baseInputPath = Paths.get(inputPath).getFileName().toString();
    String filename;
    if (totalTasks <= 0) {
      filename =
          String.format(
              "%s_bench-type=%s_sketch-params=%s.csv",
              baseInputPath, benchmarkType.toString(), sketchParams.toLowerCase());
    } else {
      filename =
          String.format(
              "%s_bench-type=%s_sketch-params=%s_task-id=%d_total_tasks=%d.csv",
              baseInputPath,
              benchmarkType.toString(),
              sketchParams.toLowerCase(),
              taskId,
              totalTasks);
    }
    Files.createDirectories(Paths.get(outputPath));
    FileWriter resultsFile = new FileWriter(Paths.get(outputPath, filename).toString());

    final Benchmark bench;
    if (benchmarkType == BenchmarkType.CORR_PERF) {
      bench = new CorrelationPerformanceBenchmark();
    } else if (benchmarkType == BenchmarkType.CORR_STATS) {
      bench = new CorrelationStatsBenchmark();
    } else if (benchmarkType == BenchmarkType.MI) {
      aggregations = Arrays.asList(AggregateFunction.MOST_FREQUENT);
      bench = new MutualInformationBenchmark();
    } else {
      throw new IllegalArgumentException("Invalid benchmark type: " + benchmarkType);
    }

    resultsFile.write(bench.csvHeader() + "\n");

    System.out.println("Total number of column combinations: " + combinations.size());
    final Stream<ColumnCombination> stream;
    final int total;
    if (totalTasks > 1) {
      List<ColumnCombination> thisTaskCombinations =
          IntStream.range(0, combinations.size())
              .filter(i -> i % totalTasks == taskId)
              .mapToObj(combinations::get)
              .collect(Collectors.toList());
      System.out.println("Column combinations for this task: " + thisTaskCombinations.size());
      stream = thisTaskCombinations.stream();
      total = thisTaskCombinations.size();
    } else {
      stream = combinations.stream();
      total = combinations.size();
    }

    final AtomicInteger processed = new AtomicInteger(0);

    Cache<String, ColumnPair> cache = CacheBuilder.newBuilder().softValues().build();

    int cores = cpuCores > 0 ? cpuCores : Runtime.getRuntime().availableProcessors();
    ForkJoinPool forkJoinPool = new ForkJoinPool(cores);
    List<AggregateFunction> finalAggregations = aggregations;
    Runnable task =
        () ->
            stream
                .parallel()
                .map(
                    (ColumnCombination columnPair) -> {
                      ColumnPair x = getColumnPair(cache, columnStore, columnPair.x);
                      ColumnPair y = getColumnPair(cache, columnStore, columnPair.y);
                      List<String> results = bench.run(x, y, sketchParamsList, finalAggregations);
                      reportProgress(processed, total);
                      return toCSV(results);
                    })
                .forEach(writeCSV(resultsFile));
    forkJoinPool.submit(task).get();

    resultsFile.close();
    columnStore.close();

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

  private ColumnPair getColumnPair(
      Cache<String, ColumnPair> cache, StringObjectKVDB<ColumnPair> db, String key) {
    ColumnPair cp = cache.getIfPresent(key);
    if (cp == null) {
      cp = db.get(key);
      cache.put(key, cp);
    }
    return cp;
  }
}
