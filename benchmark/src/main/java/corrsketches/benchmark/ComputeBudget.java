package corrsketches.benchmark;

import corrsketches.benchmark.CreateColumnStore.ColumnStoreMetadata;
import corrsketches.benchmark.utils.CliTool;
import edu.nyu.engineering.vida.kvdb4j.api.StringObjectKVDB;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = ComputeBudget.JOB_NAME, description = "")
public class ComputeBudget extends CliTool implements Serializable {

  public static final String JOB_NAME = "ComputeBudget";

  @Option(
      names = "--input-path",
      required = true,
      description = "Folder containing the column store")
  private String inputPath;

  public static void main(String[] args) {
    CliTool.run(args, new ComputeBudget());
  }

  @Override
  public void execute() throws Exception {

    ColumnStoreMetadata storeMetadata = CreateColumnStore.readMetadata(inputPath);
    StringObjectKVDB<ColumnPair> columnStore =
        CreateColumnStore.KVColumnStore.create(inputPath, DBType.ROCKSDB, true);

    Set<Set<String>> columnSets = storeMetadata.columnSets;
    System.out.println(
        "> Found  " + columnSets.size() + " column pair sets in DB stored at " + inputPath);

    Set<String> keyColumns = new HashSet<>();
    Set<String> uniqueElements = new HashSet<>();

    for (Set<String> datasetColumnPairIds : columnSets) {
      for (String columnPairId : datasetColumnPairIds) {
        ColumnPair x = columnStore.get(columnPairId);
        keyColumns.add(x.datasetId + "/" + x.keyName);
        uniqueElements.addAll(x.keyValues);
      }
    }

    int[] kValues = new int[] {128, 256, 512, 1024};
    printParameters(kValues, keyColumns.size(), uniqueElements.size(), inputPath);
  }

  public static void printParameters(
      int[] kValues, int numOfColumns, int numOfElements, String inputPath) {
    System.out.println("\nParameter equivalency for " + inputPath);

    int m = numOfColumns;
    int N = numOfElements;
    for (int k : kValues) {
      int b = k * m;
      double tau = b / (double) N;
      System.out.printf("k=%d ", k);
      System.out.printf("budget=%d*%d=%d ", k, m, b);
      System.out.printf("unique-keys=%d ", N);
      System.out.printf("unique-keys=%d ", N);
      System.out.printf("tau=%.5f", tau);
      System.out.printf("\n");
    }
  }
}
