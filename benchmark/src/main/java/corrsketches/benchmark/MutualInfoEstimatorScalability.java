package corrsketches.benchmark;

import static corrsketches.benchmark.ComputePairwiseJoinCorrelations.*;
import static java.util.Arrays.asList;
import static picocli.CommandLine.*;

import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.datasource.MultinomialSyntheticSource;
import corrsketches.benchmark.pairwise.ColumnCombination;
import corrsketches.benchmark.params.SketchParams;
import corrsketches.benchmark.utils.CliTool;
import java.io.Serializable;
import java.util.*;

@Command(
    name = corrsketches.benchmark.MutualInfoEstimatorScalability.JOB_NAME,
    description = "Creates a column index to be used by the sketching benchmark")
public class MutualInfoEstimatorScalability extends CliTool implements Serializable {

  public static final String JOB_NAME = "MutualInfoEstimatorScalability";

  @Option(names = "--output-path", required = true, description = "Output path for results file")
  String outputPath;

  @Option(
      names = "--sketch-params",
      required = true,
      description =
          "A comma-separated list of sketch parameters in the format SKETCH_TYPE:BUDGET_VALUE,SKETCH_TYPE:BUDGET_VALUE,...")
  String sketchParams = null;

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

  //  @Option(
  //      names = "--distribution",
  //      description =
  //          "The distribution used to generate the data. Options: \n"
  //              + "1. \"SBN\" for Bivariate Normal.\n"
  //              + "2. \"MNL\" for Multinomial (default)\n"
  //              + "3. \"CDU\" for Continuous-Discrete Uniform\n")
  //  String distribution = "MNL";

  public static void main(String[] args) {
    CliTool.run(args, new CreateColumnStore());
  }

  @Override
  public void execute() throws Exception {
    List<SketchParams> sketchParamsList = SketchParams.parse(this.sketchParams);
    System.out.println("> SketchParams: " + this.sketchParams);

    List<AggregateFunction> leftAggregations = List.of(AggregateFunction.NONE);
    System.out.println("> LEFT aggregate functions: " + leftAggregations);

    List<AggregateFunction> rightAggregations = List.of(AggregateFunction.FIRST);
    System.out.println("> RIGHT aggregate functions: " + rightAggregations);

    BenchmarkType benchmarkType = BenchmarkType.MI_PERF;
    String distribution = "MNL";
    String sketchParams = "TUPSK";
    int totalTasks = -1;
    int taskId = -1;
    int cpuCores = -1;
    int randomSeed = 1234;
    int samples = 10;

    // Set up data source
    System.out.println("\n> Computing column statistics for all column combinations...");

    List<ColumnCombination> combinations = new ArrayList<>();
    for(var sizes : asList(5000, 10000, 20000)) {
      combinations.addAll(MultinomialSyntheticSource.createColumnCombinations(samples, randomSeed, sizes));
    }

    // Initialize the output filename
    String datasetName = distribution.toLowerCase();

    String filename =
        String.format(
            "%s_bench-type=%s_sketch-params=%s.csv",
            datasetName, benchmarkType, sketchParams.toLowerCase());

    // Set up the benchmark type
    final Benchmark bench = new MutualInformationBenchmarkPerf(sketchParamsList, leftAggregations, rightAggregations);
    BaseBenchmark.runParallel(
        totalTasks, taskId, cpuCores, combinations, bench, outputPath, filename);

    System.out.println(getClass().getSimpleName() + " finished successfully.");
  }

}
