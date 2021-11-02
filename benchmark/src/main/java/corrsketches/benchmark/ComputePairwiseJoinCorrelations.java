package corrsketches.benchmark;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import corrsketches.SketchType;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.CreateColumnStore.ColumnStoreMetadata;
import corrsketches.benchmark.utils.CliTool;
import hashtabledb.BytesBytesHashtable;
import hashtabledb.Kryos;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = ComputePairwiseJoinCorrelations.JOB_NAME,
    description = "Compute correlation after joins of each pair of categorical-numeric column")
public class ComputePairwiseJoinCorrelations extends CliTool implements Serializable {

  public static final String JOB_NAME = "ComputePairwiseJoinCorrelations";

  public static final Kryos<ColumnPair> KRYO = new Kryos<>(ColumnPair.class);

  enum BenchmarkType {
    CORR_STATS,
    CORR_PERF
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
    List<SketchParams> sketchParamsList = SketchParams.parse(this.sketchParams);
    System.out.println("> SketchParams: " + this.sketchParams);

    List<AggregateFunction> aggregations = parseAggregations(this.aggregateFunctions);
    System.out.println("> Using aggregate functions: " + aggregations);

    ColumnStoreMetadata storeMetadata = CreateColumnStore.readMetadata(inputPath);
    BytesBytesHashtable columnStore = new BytesBytesHashtable(storeMetadata.dbType, inputPath);

    Set<Set<String>> columnSets = storeMetadata.columnSets;
    System.out.println(
        "> Found  " + columnSets.size() + " column pair sets in DB stored at " + inputPath);

    System.out.println("\n> Computing column statistics for all column combinations...");
    Set<ColumnCombination> combinations =
        ColumnCombination.createColumnCombinations(
            columnSets, intraDatasetCombinations, maxSamples);

    String baseInputPath = Paths.get(inputPath).getFileName().toString();
    String filename =
        String.format(
            "%s_bench-type=%s_sketch-params=%s.csv",
            baseInputPath, benchmarkType.toString(), sketchParams.toLowerCase());

    Files.createDirectories(Paths.get(outputPath));
    FileWriter resultsFile = new FileWriter(Paths.get(outputPath, filename).toString());

    final Benchmark bench;
    if (benchmarkType == BenchmarkType.CORR_PERF) {
      bench = new CorrelationPerformanceBenchmark();
    } else if (benchmarkType == BenchmarkType.CORR_STATS) {
      bench = new CorrelationStatsBenchmark();
    } else {
      throw new IllegalArgumentException("Invalid benchmark type: " + benchmarkType);
    }

    resultsFile.write(bench.csvHeader() + "\n");

    System.out.println("Number of combinations: " + combinations.size());
    final AtomicInteger processed = new AtomicInteger(0);
    final int total = combinations.size();

    Cache<String, ColumnPair> cache = CacheBuilder.newBuilder().softValues().build();

    int cores = cpuCores > 0 ? cpuCores : Runtime.getRuntime().availableProcessors();
    ForkJoinPool forkJoinPool = new ForkJoinPool(cores);
    List<AggregateFunction> finalAggregations = aggregations;
    Runnable task =
        () ->
            combinations.stream()
                .parallel()
                .map(
                    (ColumnCombination columnPair) -> {
                      ColumnPair x = getColumnPair(cache, columnStore, columnPair.x);
                      ColumnPair y = getColumnPair(cache, columnStore, columnPair.y);
                      List<String> results = bench.run(x, y, sketchParamsList, finalAggregations);
                      return toCSV(processed, total, results);
                    })
                .forEach(writeCSV(resultsFile));
    forkJoinPool.submit(task).get();

    resultsFile.close();
    columnStore.close();

    System.out.println(getClass().getSimpleName() + " finished successfully.");
  }

  private List<AggregateFunction> parseAggregations(String functions) {
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

  private String toCSV(AtomicInteger processed, double total, List<String> results) {
    int current = processed.incrementAndGet();
    if (current % 1000 == 0) {
      double percent = 100 * current / total;
      synchronized (System.out) {
        System.out.printf("Progress: %.3f%%\n", percent);
      }
    }

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

  private ColumnPair getColumnPair(
      Cache<String, ColumnPair> cache, BytesBytesHashtable hashtable, String key) {
    ColumnPair cp = cache.getIfPresent(key);
    if (cp == null) {
      byte[] keyBytes = key.getBytes();
      cp = KRYO.unserializeObject(hashtable.get(keyBytes));
      cache.put(key, cp);
    }
    return cp;
  }

  private Consumer<String> writeCSV(FileWriter file) {
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

  public static class SketchParams {

    public final SketchType type;
    public final double budget;

    public SketchParams(SketchType type, double budget) {
      this.type = type;
      this.budget = budget;
    }

    public static List<SketchParams> parse(String params) {
      String[] values = params.split(",");
      List<SketchParams> result = new ArrayList<>();
      for (String value : values) {
        result.add(parseValue(value.trim()));
      }
      if (result.isEmpty()) {
        throw new IllegalArgumentException(
            String.format("[%s] does not have any valid sketch parameters", params));
      }
      return result;
    }

    public static SketchParams parseValue(String params) {
      String[] values = params.split(":");
      if (values.length == 2) {
        return new SketchParams(
            SketchType.valueOf(values[0].trim()), Double.parseDouble(values[1].trim()));
      } else {
        throw new IllegalArgumentException(String.format("[%s] is not a valid parameter", params));
      }
    }

    @Override
    public String toString() {
      return type.toString() + ":" + budget;
    }
  }
}
