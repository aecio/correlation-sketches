package benchmark;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.Sets;
import hashtabledb.BytesBytesHashtable;
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
import benchmark.BenchmarkUtils.Result;
import utils.CliTool;

@Command(
    name = ComputePairwiseCorrelationJoinsThreads.JOB_NAME,
    description = "Compute correlation after joins of each pair of categorical-numeric column")
public class ComputePairwiseCorrelationJoinsThreads extends CliTool implements Serializable {

  public static final String JOB_NAME = "ComputePairwiseCorrelationJoinsThreads";

  public static final Kryos<ColumnPair> KRYO = new Kryos(ColumnPair.class);

  @Required
  @Option(name = "--input-path", description = "Folder containing CSV files")
  private String inputPath;

  @Required
  @Option(name = "--output-path", description = "Output path for results file")
  private String outputPath;

  @Required
  @Option(name = "--num-hashes", description = "Number of hashes per sketch")
  private int numHashes;

  public static void main(String[] args) {
    CliTool.run(args, new ComputePairwiseCorrelationJoinsThreads());
  }

  @Override
  public void execute() throws Exception {

    Path db = Paths.get(outputPath, "db");
    BytesBytesHashtable hashtable = new BytesBytesHashtable(db.toString());
    System.out.println("Created DB at " + db.toString());

    List<String> allCSVs = BenchmarkUtils.findAllCSVs(inputPath);
    System.out.println("> Found  " + allCSVs.size() + " CSV files at " + inputPath);

    System.out.println("\n> Writing columns to key-value DB...");
    Set<String> allColumns = new HashSet<>();
    for (String csv : allCSVs) {
      Set<ColumnPair> columnPairs = BenchmarkUtils.readColumnPairs(csv);
      for (ColumnPair cp : columnPairs) {
        String id = cp.id();
        hashtable.put(id.getBytes(), KRYO.serializeObject(cp));
        allColumns.add(id);
      }
    }

    System.out.println("\n> Computing column statistics for all column combinations...");

    Set<Set<String>> combinations = Sets.combinations(allColumns, 2);
    FileWriter f = new FileWriter(Paths.get(outputPath, "results.csv").toString());
    f.write(Result.csvHeader() + "\n");

    System.out.println("Number of combinations: " + combinations.size());
    final AtomicInteger processed = new AtomicInteger(0);
    final int total = combinations.size();
    //    int cores = Runtime.getRuntime().availableProcessors();
    int cores = 4;
    ForkJoinPool forkJoinPool = new ForkJoinPool(cores);

    forkJoinPool
        .submit(
            () ->
                combinations
                    .stream()
                    .parallel()
                    .map(
                        (Set<String> columnPair) -> {
                          Iterator<String> iterator = columnPair.iterator();
                          byte[] cp1Id = iterator.next().getBytes();
                          byte[] cp2Id = iterator.next().getBytes();
                          ColumnPair columnPair1 = KRYO.unserializeObject(hashtable.get(cp1Id));
                          ColumnPair columnPair2 = KRYO.unserializeObject(hashtable.get(cp2Id));
                          Result result =
                              BenchmarkUtils.computeStatistics(numHashes, columnPair1, columnPair2);

                          int current = processed.incrementAndGet();
                          if (current % 100 == 0) {
                            double percent = 100 * current / (double) total;
                            synchronized (System.out) {
                              System.out.printf("Progress: %.3f%%\n", percent);
                            }
                          }
                          return result == null ? "" : result.csvLine();
                        })
                    .forEach(
                        (String line) -> {
                          if (!line.isEmpty()) {
                            synchronized (f) {
                              try {
                                f.write(line);
                                f.write("\n");
                                f.flush();
                              } catch (IOException e) {
                                throw new RuntimeException("Failed to write line to file: " + line);
                              }
                            }
                          }
                        }))
        .get();
    f.close();
    System.out.println(getClass().getSimpleName() + " finished successfully.");
  }
}
