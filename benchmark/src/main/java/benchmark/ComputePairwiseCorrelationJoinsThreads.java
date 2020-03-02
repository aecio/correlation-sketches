package benchmark;

import benchmark.BenchmarkUtils.Result;
import benchmark.CreateColumnStore.ColumnStoreMetadata;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import hashtabledb.BytesBytesHashtable;
import hashtabledb.Kryos;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import sketches.correlation.SketchType;
import utils.CliTool;

@Command(
    name = ComputePairwiseCorrelationJoinsThreads.JOB_NAME,
    description = "Compute correlation after joins of each pair of categorical-numeric column")
public class ComputePairwiseCorrelationJoinsThreads extends CliTool implements Serializable {

  public static final String JOB_NAME = "ComputePairwiseCorrelationJoinsThreads";

  public static final Kryos<ColumnPair> KRYO = new Kryos(ColumnPair.class);

  @Required
  @Option(name = "--input-path", description = "Folder containing key-value column store")
  String inputPath;

  @Required
  @Option(name = "--output-path", description = "Output path for results file")
  String outputPath;

  @Required
  @Option(
      name = "--sketch-params",
      description =
          "A comma-separated list of sketch parameters in the format SKETCH_TYPE:BUDGET_VALUE,SKETCH_TYPE:BUDGET_VALUE,...")
  String sketchParams = null;

  @Option(
      name = "--intra-dataset-combinations",
      description = "Whether to consider only intra-dataset column combinations")
  Boolean intraDatasetCombinations = false;

  @Option(
      name = "--max-combinations",
      description = "The maximum number of columns to consider for creating combinations.")
  private int maxSamples = 5000;

  @Option(
      name = "--cpu-cores",
      description = "Number of CPU core to use. Default is to use all cores available.")
  int cpuCores = -1;

  public static void main(String[] args) {
    CliTool.run(args, new ComputePairwiseCorrelationJoinsThreads());
  }

  @Override
  public void execute() throws Exception {
    System.out.println("sketchParams: " + this.sketchParams);
    List<SketchParams> sketchParamsList = SketchParams.parse(this.sketchParams);
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
        String.format("%s_sketch-params=%s.csv", baseInputPath, sketchParams.toLowerCase());

    Files.createDirectories(Paths.get(outputPath));
    FileWriter resultsFile = new FileWriter(Paths.get(outputPath, filename).toString());

    resultsFile.write(Result.csvHeader() + "\n");

    System.out.println("Number of combinations: " + combinations.size());
    final AtomicInteger processed = new AtomicInteger(0);
    final int total = combinations.size();

    Cache<String, ColumnPair> cache = CacheBuilder.newBuilder().softValues().build();

    int cores = cpuCores > 0 ? cpuCores : Runtime.getRuntime().availableProcessors();
    ForkJoinPool forkJoinPool = new ForkJoinPool(cores);
    forkJoinPool
        .submit(
            () ->
                combinations
                    .stream()
                    .parallel()
                    .map(computeStatistics(cache, columnStore, processed, total, sketchParamsList))
                    .forEach(writeCSV(resultsFile)))
        .get();
    resultsFile.close();
    columnStore.close();
    System.out.println(getClass().getSimpleName() + " finished successfully.");
  }

  private Function<ColumnCombination, String> computeStatistics(
      Cache<String, ColumnPair> cache,
      BytesBytesHashtable hashtable,
      AtomicInteger processed,
      double total,
      List<SketchParams> params) {
    return (ColumnCombination columnPair) -> {
      ColumnPair x = cache.getIfPresent(columnPair.x);
      if (x == null) {
        byte[] xId = columnPair.x.getBytes();
        x = KRYO.unserializeObject(hashtable.get(xId));
        cache.put(columnPair.x, x);
      }
      ColumnPair y = cache.getIfPresent(columnPair.y);
      if (y == null) {
        byte[] yId = columnPair.y.getBytes();
        y = KRYO.unserializeObject(hashtable.get(yId));
        cache.put(columnPair.y, y);
      }

      List<Result> results = BenchmarkUtils.computeStatistics(x, y, params);

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
        for (Result result : results) {
          builder.append(result.csvLine());
          builder.append('\n');
        }
        return builder.toString();
      }
    };
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
      for (int i = 0; i < values.length; i++) {
        result.add(parseValue(values[i].trim()));
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
            SketchType.valueOf(values[0].trim()), Double.valueOf(values[1].trim()));
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
