package corrsketches.benchmark;

import static corrsketches.benchmark.ComputePairwiseJoinCorrelations.*;

import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.datasource.BivariateNormalSyntheticSource;
import corrsketches.benchmark.datasource.ContDiscUnifSyntheticSource;
import corrsketches.benchmark.datasource.MultinomialSyntheticSource;
import corrsketches.benchmark.pairwise.ColumnCombination;
import corrsketches.benchmark.params.SketchParams;
import corrsketches.benchmark.utils.CliTool;
import java.io.Serializable;
import java.util.List;
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
  int samples = 1000;

  @Option(names = "--total-tasks", description = "The to number of tasks to split the computation.")
  int totalTasks = -1;

  @Option(
      names = "--task-id",
      description = "The id of this task, a number in the range [0, <total-tasks> - 1]")
  int taskId = -1;

  @Option(
      names = "--cpu-cores",
      description = "Number of CPU core to use. Default is to use all cores available.")
  int cpuCores = -1;

  @Option(names = "--seed", description = "A seed for the random number generator.")
  int randomSeed = 1234;

  @Option(
      names = "--right-aggregations",
      description = "Aggregation functions for the RIGHT table separated by comma (,), or \"all\"")
  String rightAggregateFunctions = "FIRST";

  @Option(
      names = "--left-aggregations",
      description = "Aggregation functions for the LEFT table separated by comma (,), or \"all\"")
  String leftAggregateFunctions = "NONE";

  @Option(
      names = "--distribution",
      description =
          "The distribution used to generate the data. Options: \n"
              + "1. \"SBN\" for Bivariate Normal.\n"
              + "2. \"MNL\" for Multinomial (default)\n"
              + "3. \"CDU\" for Continuous-Discrete Uniform\n")
  String distribution = "MNL";

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

    List<AggregateFunction> leftAggregations = parseAggregations(this.leftAggregateFunctions);
    System.out.println("> LEFT aggregate functions: " + leftAggregations);

    List<AggregateFunction> rightAggregations = parseAggregations(this.rightAggregateFunctions);
    System.out.println("> RIGHT aggregate functions: " + rightAggregations);

    BenchmarkType benchmarkType = BenchmarkType.MI;

    // Set up data source
    System.out.println("\n> Computing column statistics for all column combinations...");
    List<ColumnCombination> combinations;
    if ("MNL".equals(distribution)) {
      System.out.println("  (Using Multinomial distribution)");
      combinations = MultinomialSyntheticSource.createColumnCombinations(samples, randomSeed);
    } else if ("SBN".equals(distribution)) {
      System.out.println("  (Using Bivariate Normal distribution)");
      combinations = BivariateNormalSyntheticSource.createColumnCombinations(samples, randomSeed);
    } else if ("CDU".equals(distribution)) {
      System.out.println("  (Using Continuous-Discrete Uniform Distributions))");
      combinations = ContDiscUnifSyntheticSource.createColumnCombinations(samples, randomSeed);
    } else {
      throw new IllegalArgumentException("Unsupported distributions: " + distribution);
    }

    // Initialize the output filename
    String datasetName = distribution.toLowerCase();
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
    final Benchmark bench =
        new MutualInformationBenchmark(sketchParamsList, leftAggregations, rightAggregations);

    BaseBenchmark.runParallel(
        totalTasks, taskId, cpuCores, combinations, bench, outputPath, filename);

    System.out.println(getClass().getSimpleName() + " finished successfully.");
  }
}
