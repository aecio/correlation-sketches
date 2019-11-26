package benchmark;

import benchmark.BenchmarkUtils.Result;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.Sets;
import hashtabledb.BytesBytesHashtable;
import hashtabledb.DBType;
import hashtabledb.Kryos;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import sketches.correlation.Sketches.Type;
import sketches.kmv.KMV;
import utils.CliTool;

@Command(
    name = ComputePairwiseCorrelationJoinsThreads.JOB_NAME,
    description = "Compute correlation after joins of each pair of categorical-numeric column")
public class ComputePairwiseCorrelationJoinsThreads extends CliTool implements Serializable {

  public static final String JOB_NAME = "ComputePairwiseCorrelationJoinsThreads";

  public static final Kryos<ColumnPair> KRYO = new Kryos(ColumnPair.class);

  @Required
  @Option(name = "--input-path", description = "Folder containing CSV files")
  String inputPath;

  @Required
  @Option(name = "--output-path", description = "Output path for results file")
  String outputPath;

  @Option(name = "--sketch-type", description = "The type sketch to be used")
  Type sketch = Type.KMV;

  @Required
  @Option(name = "--num-hashes", description = "Number of hashes per sketch")
  double numHashes = KMV.DEFAULT_K;

  @Option(name = "--min-rows", description = "Minimum number of rows to consider table")
  int minRows = 1;

  @Option(name = "--db-backend", description = "")
  DBType dbType = DBType.LEVELDB;

  @Option(
      name = "--intra-dataset-combinations",
      description = "Whether to consider only intra-dataset column combinations")
  Boolean intraDatasetCombinations = false;

  public static void main(String[] args) {
    CliTool.run(args, new ComputePairwiseCorrelationJoinsThreads());
  }

  @Override
  public void execute() throws Exception {
    Path db = Paths.get(outputPath, "db");

    BytesBytesHashtable hashtable = new BytesBytesHashtable(dbType, db.toString());
    System.out.println("Created DB at " + db.toString());

    List<String> allCSVs = BenchmarkUtils.findAllCSVs(inputPath);
    System.out.println("> Found  " + allCSVs.size() + " CSV files at " + inputPath);

    System.out.println("\n> Writing columns to key-value DB...");
    Set<Set<String>> allColumns = new HashSet<>();
    for (String csv : allCSVs) {
      Set<ColumnPair> columnPairs = BenchmarkUtils.readColumnPairs(csv, minRows);
      Set<String> columnIds = new HashSet<>();
      for (ColumnPair cp : columnPairs) {
//        String id = cp.id();
        String id = cp.toString();
        hashtable.put(id.getBytes(), KRYO.serializeObject(cp));
        columnIds.add(id);
      }
      allColumns.add(columnIds);
    }

    System.out.println("\n> Computing column statistics for all column combinations...");
    Set<ColumnCombination> combinations =
        createColumnCombinations(allColumns, intraDatasetCombinations);

    String baseInputPath = Paths.get(inputPath).getFileName().toString();
    String filename =
        String.format("%s_sketch=%s_b=%.3f.csv",
              baseInputPath,
              sketch.toString().toLowerCase(),
              numHashes
        );
    FileWriter f = new FileWriter(Paths.get(outputPath, filename).toString());
    f.write(Result.csvHeader() + "\n");

    System.out.println("Number of combinations: " + combinations.size());
    final AtomicInteger processed = new AtomicInteger(0);
    final int total = combinations.size();
    //    int cores = Runtime.getRuntime().availableProcessors();
    int cores = 8;
    ForkJoinPool forkJoinPool = new ForkJoinPool(cores);
    forkJoinPool
        .submit(
            () ->
                combinations.stream()
                    .parallel()
                    .map(computeStatistics(hashtable, processed, total, this.sketch))
                    .forEach(writeCSV(f)))
        .get();
    f.close();
    hashtable.close();
    System.out.println(getClass().getSimpleName() + " finished successfully.");
  }

  private Set<ColumnCombination> createColumnCombinations(
      Set<Set<String>> allColumns, Boolean intraDatasetCombinations) {

    Set<ColumnCombination> result = new HashSet<>();

    if (intraDatasetCombinations) {
      for (Set<String> c : allColumns) {
        Set<Set<String>> intraCombinations = Sets.combinations(c, 2);
        for (Set<String> columnPair : intraCombinations) {
          result.add(createColumnCombination(columnPair));
        }
      }
    } else {
      Set<String> columnsSet = new HashSet<>();
      for (Set<String> c : allColumns) {
        columnsSet.addAll(c);
      }
      Set<Set<String>> interCombinations = Sets.combinations(columnsSet, 2);
      for (Set<String> columnPair : interCombinations) {
        result.add(createColumnCombination(columnPair));
      }
    }
    return result;
  }

  private ColumnCombination createColumnCombination(Set<String> columnPair) {
    Iterator<String> it = columnPair.iterator();
    String x = it.next();
    String y = it.next();
    return new ColumnCombination(x, y);
  }

  private Function<ColumnCombination, String> computeStatistics(
      BytesBytesHashtable hashtable, AtomicInteger processed, double total, Type sketch) {
    return (ColumnCombination columnPair) -> {
      byte[] xId = columnPair.x.getBytes();
      byte[] yId = columnPair.y.getBytes();
      ColumnPair x = KRYO.unserializeObject(hashtable.get(xId));
      ColumnPair y = KRYO.unserializeObject(hashtable.get(yId));

      Result result = BenchmarkUtils.computeStatistics(x, y, sketch, numHashes);

      int current = processed.incrementAndGet();
      if (current % 100 == 0) {
        double percent = 100 * current / total;
        synchronized (System.out) {
          System.out.printf("Progress: %.3f%%\n", percent);
        }
      }
      return result == null ? "" : result.csvLine();
    };
  }

  private Consumer<String> writeCSV(FileWriter file) {
    return (String line) -> {
      if (!line.isEmpty()) {
        synchronized (file) {
          try {
            file.write(line);
            file.write("\n");
            file.flush();
          } catch (IOException e) {
            throw new RuntimeException("Failed to write line to file: " + line);
          }
        }
      }
    };
  }

  public static class ColumnCombination {
    public String x;
    public String y;

    public ColumnCombination(String x, String y) {
      this.x = x;
      this.y = y;
    }
  }
}
