package corrsketches.benchmark;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.params.SketchParams;
import corrsketches.benchmark.utils.CliTool;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import picocli.CommandLine;

import java.io.FileWriter;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static corrsketches.benchmark.ComputePairwiseJoinCorrelations.*;

@CommandLine.Command(
        name = SyntheticMutualInfoBenchmark.JOB_NAME,
        description = "Compute correlation after joins of each pair of categorical-numeric column")
public class SyntheticMutualInfoBenchmark extends CliTool implements Serializable {

//  public static void main(String[] args) {
//    //
//    int seed = 12345;
//    double r = -1;
//    int sampleSize = 1000;
//
//    double[][] sample = sampleBivariateNormal(sampleSize, r, seed);
//
//    String datasetId = ;
//    x = new ColumnPair(datasetId
//            keyName,
//            List<String> keyValues,
//            String columnName,
//            ColumnType valueType
//    );
//
//    //    System.out.println(Arrays.deepToString(sample));
//    System.out.println(sample.length);
//    System.out.println(sample[0].length);
//    System.out.println(PearsonCorrelation.coefficient(sample[0], sample[1]));
//    System.out.printf("r=%f\n", r);
//  }

  public static final String JOB_NAME = "SyntheticMutualInfoBenchmark";

//  enum BenchmarkType {
//    CORR_STATS,
//    CORR_PERF,
//    MI
//  }

  @CommandLine.Option(
          names = "--input-path",
          required = true,
          description = "Folder containing key-value column store")
  String inputPath;

  @CommandLine.Option(names = "--output-path", required = true, description = "Output path for results file")
  String outputPath;

  @CommandLine.Option(
          names = "--sketch-params",
          required = true,
          description =
                  "A comma-separated list of sketch parameters in the format SKETCH_TYPE:BUDGET_VALUE,SKETCH_TYPE:BUDGET_VALUE,...")
  String sketchParams = null;

//  @CommandLine.Option(
//          names = "--intra-dataset-combinations",
//          description = "Whether to consider only intra-dataset column combinations")
//  Boolean intraDatasetCombinations = false;
//
//  @CommandLine.Option(names = "--bench", description = "The type of benchmark to run")
//  ComputePairwiseJoinCorrelations.BenchmarkType benchmarkType = ComputePairwiseJoinCorrelations.BenchmarkType.CORR_STATS;
//
//  @CommandLine.Option(
//          names = "--max-combinations",
//          description = "The maximum number of columns to consider for creating combinations.")
//  private int maxSamples = 5000;

  @CommandLine.Option(names = "--total-tasks", description = "The to number of tasks to split the computation.")
  private int totalTasks = -1;

  @CommandLine.Option(
          names = "--task-id",
          description = "The id of this task, a number in the range [0, <total-tasks> - 1]")
  private int taskId = -1;

  @CommandLine.Option(
          names = "--cpu-cores",
          description = "Number of CPU core to use. Default is to use all cores available.")
  int cpuCores = -1;

  @CommandLine.Option(names = "--aggregations", description = "Run performance experiments")
  String aggregateFunctions = "FIRST";

  public static void main(String[] args) {
    CliTool.run(args, new SyntheticMutualInfoBenchmark());
  }

  @Override
  public void execute() throws Exception {
    if (totalTasks > 0 && taskId < 0) {
      System.out.printf("taskId=[%d] must be a number from 0 to %d (total-tasks)\n", totalTasks);
      System.exit(1);
    }

    List<SketchParams> sketchParamsList = SketchParams.parse(this.sketchParams);
    System.out.println("> SketchParams: " + this.sketchParams);

    List<AggregateFunction> aggregations = ComputePairwiseJoinCorrelations.parseAggregations(this.aggregateFunctions);
    System.out.println("> Using aggregate functions: " + aggregations);


    String baseInputPath = Paths.get(inputPath).getFileName().toString();
    String filename;
    String benchmarkType = "MI_SBN";
    if (totalTasks <= 0) {
      filename =
              String.format(
                      "%s_bench-type=%s_sketch-params=%s.csv",
                      baseInputPath, benchmarkType, sketchParams.toLowerCase());
    } else {
      filename =
              String.format(
                      "%s_bench-type=%s_sketch-params=%s_task-id=%d_total_tasks=%d.csv",
                      baseInputPath,
                      benchmarkType,
                      sketchParams.toLowerCase(),
                      taskId,
                      totalTasks);
    }
    Files.createDirectories(Paths.get(outputPath));
    FileWriter resultsFile = new FileWriter(Paths.get(outputPath, filename).toString());


    final Benchmark bench = new MutualInformationBenchmark(true);
    resultsFile.write(bench.csvHeader() + "\n");


    System.out.println("\n> Computing column statistics for all column combinations...");
    int samples = 10000;
    List<ColumnCombination> combinations = ColumnCombination.createColumnCombinations(samples);

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

    System.out.println(getClass().getSimpleName() + " finished successfully.");
  }

  static class ColumnCombination {
    public final String x;
    public final String y;

    public ColumnCombination(String x, String y) {
      this.x = x;
      this.y = y;
    }

    public static List<ColumnCombination> createColumnCombinations(int samples) {
      // TODO
      return null;
    }
  }

  /**
   * Samples data points from a bivariate normal distribution with the given correlation.
   *
   * @param sampleSize the number of samples to generate
   * @param correlation the desired level of correlation (Pearson's correlation)
   * @param seed a seed for the random number generator
   * @return a matrix of dimensions (2, sampleSize) containing samples of the
   */
  private static double[][] sampleBivariateNormal(int sampleSize, double correlation, int seed) {
    // Correlation equal to +1 or -1 leads to a singular matrix, which causes a SingularMatrixException,
    // so we 'round' down/up them to their closest values.
    final double r;
    if (correlation == 1) {
      r = Math.nextDown(1);
    } else if (correlation == -1) {
      r = Math.nextUp(-1);
    } else{
      r = correlation;
    }
    double[] means = new double[] {0, 0};
    double[][] covariances =
        new double[][] {
          new double[] {1, r},
          new double[] {r, 1}
        };
    RandomGenerator rng = new JDKRandomGenerator(seed);
    var mnd = new MultivariateNormalDistribution(rng, means, covariances);
    return transposeMatrix(mnd.sample(sampleSize));
  }

  /**
   * Transposes a matrix.
   *
   * @param data
   * @return
   */
  public static double[][] transposeMatrix(double[][] data) {
    final int n = data[0].length;
    final int m = data.length;
    double[][] transposed = new double[n][m];
    for (int i = 0; i < m; i++) {
      for (int j = 0; j < n; j++) {
        transposed[j][i] = data[i][j];
      }
    }
    return transposed;
  }
}
