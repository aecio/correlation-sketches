package corrsketches.benchmark;

import static corrsketches.benchmark.ComputePairwiseJoinCorrelations.*;

import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.datasource.MultinomialSyntheticSource;
import corrsketches.benchmark.pairwise.ColumnCombination;
import corrsketches.benchmark.params.SketchParams;
import corrsketches.benchmark.utils.CliTool;
import java.io.FileWriter;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = SyntheticPairwiseMutualInfoBenchmark.JOB_NAME,
    description = "Compute correlation after joins of each pair of categorical-numeric column")
public class SyntheticPairwiseMutualInfoBenchmark extends CliTool implements Serializable {

  public static final String JOB_NAME = "SyntheticPairwiseMutualInfoBenchmark";

  @Option(names = "--output-path", required = true, description = "Output path for results file")
  String outputPath;

  @Option(
      names = "--sketch-params",
      required = true,
      description =
          "A comma-separated list of sketch parameters in the format SKETCH_TYPE:BUDGET_VALUE,SKETCH_TYPE:BUDGET_VALUE,...")
  String sketchParams = null;

  @Option(names = "--num-samples", description = "The number of column combinations to generate.")
  private int samples = 1000;

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

  @Option(
      names = "--aggregations",
      description = "Aggregation functions separated by comma (,), or \"all\"")
  String aggregateFunctions = "FIRST";

  public static void main(String[] args) {
    CliTool.run(args, new SyntheticPairwiseMutualInfoBenchmark());
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

    BenchmarkType benchmarkType = BenchmarkType.MI;

    // Set up data source
    System.out.println("\n> Computing column statistics for all column combinations...");
    //    List<ColumnCombination> combinations = SyntheticSource.createColumnCombinations(samples);
    List<ColumnCombination> combinations =
        MultinomialSyntheticSource.createColumnCombinations(samples);

    // Initialize the output filename
    String datasetName = "sbn";
    String filename;
    if (totalTasks <= 0) {
      filename =
          String.format(
              "%s_bench-type=%s_sketch-params=%s.csv",
              datasetName, benchmarkType, sketchParams.toLowerCase());
    } else {
      filename =
          String.format(
              "%s_bench-type=%s_sketch-params=%s_task-id=%d_total_tasks=%d.csv",
              datasetName, benchmarkType, sketchParams.toLowerCase(), taskId, totalTasks);
    }

    // Set up the benchmark type
    final Benchmark bench;
    //    if (benchmarkType == BenchmarkType.CORR_PERF) {
    //      bench = new CorrelationPerformanceBenchmark();
    //    } else if (benchmarkType == BenchmarkType.CORR_STATS) {
    //      bench = new CorrelationStatsBenchmark();
    //    } else if (benchmarkType == BenchmarkType.MI) {
    //      aggregations = Arrays.asList(AggregateFunction.MOST_FREQUENT);
    //      bench = new MutualInformationBenchmark();
    //    } else {
    //      throw new IllegalArgumentException("Invalid benchmark type: " + benchmarkType);
    //    }
    aggregations = Arrays.asList(AggregateFunction.FIRST);
    bench = new MutualInformationBenchmark();

    // Initialize CSV output file and start writing headers
    Files.createDirectories(Paths.get(outputPath));
    FileWriter resultsFile = new FileWriter(Paths.get(outputPath, filename).toString());
    resultsFile.write(bench.csvHeader() + "\n");

    // If necessary, filter combinations leaving only the ones that should be computed by this task
    System.out.println("Total number of column combinations: " + combinations.size());
    if (totalTasks > 1) {
      combinations =
          IntStream.range(0, combinations.size())
              .filter(i -> i % totalTasks == taskId)
              .mapToObj(combinations::get)
              .collect(Collectors.toList());
      System.out.println("Column combinations for this task: " + combinations.size());
    }

    final Stream<ColumnCombination> stream = combinations.stream();
    final int total = combinations.size();
    final AtomicInteger processed = new AtomicInteger(0);
    final int cores = cpuCores > 0 ? cpuCores : Runtime.getRuntime().availableProcessors();

    ForkJoinPool forkJoinPool = new ForkJoinPool(cores);
    List<AggregateFunction> finalAggregations = aggregations;
    Runnable task =
        () ->
            stream
                .parallel()
                .map(
                    (ColumnCombination combination) -> {
                      List<String> results =
                          bench.run(combination, sketchParamsList, finalAggregations);
                      reportProgress(processed, total);
                      return toCSV(results);
                    })
                .forEach(writeCSV(resultsFile));
    forkJoinPool.submit(task).get();

    resultsFile.close();

    System.out.println(getClass().getSimpleName() + " finished successfully.");
  }
}
