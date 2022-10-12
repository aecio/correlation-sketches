package corrsketches.benchmark;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
    subcommands = {
      ComputePairwiseJoinCorrelations.class,
      CreateColumnStore.class,
      ComputeBudget.class,
      IndexCorrelationBenchmark.class,
      SyntheticPairwiseMutualInfoBenchmark.class,
    })
public class Main {

  public static void main(String[] args) {
    System.exit(new CommandLine(new Main()).execute(args));
  }
}
