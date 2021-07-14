package corrsketches.benchmark;

import corrsketches.SketchType;
import corrsketches.benchmark.CreateColumnStore.ColumnStoreMetadata;
import corrsketches.benchmark.index.SketchIndex;
import corrsketches.benchmark.index.SketchIndex.Hit;
import corrsketches.benchmark.utils.CliTool;
import corrsketches.kmv.KMV;
import hashtabledb.BytesBytesHashtable;
import hashtabledb.KV;
import hashtabledb.Kryos;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = IndexCorrelationBenchmark.JOB_NAME,
    description = "Creates a Lucene index of tables")
public class IndexCorrelationBenchmark extends CliTool implements Serializable {

  public static final String JOB_NAME = "IndexCorrelationBenchmark";

  public static final Kryos<ColumnPair> KRYO = new Kryos<>(ColumnPair.class);

  @Option(
      names = "--input-path",
      required = true,
      description = "Folder containing key-value column store")
  String inputPath;

  @Option(names = "--output-path", required = true, description = "Output path for results file")
  String outputPath;

  //  @Option(names = "--estimator", description = "The correlation estimator to be used")
  //  CorrelationType estimator = CorrelationType.PEARSONS;

  @Option(names = "--sketch-type", description = "The type sketch to be used")
  SketchType sketchType = SketchType.KMV;

  @Option(names = "--num-queries", description = "The numbers of queries to be run")
  int numQueries = 100;

  @Option(names = "--num-hashes", required = true, description = "Number of hashes per sketch")
  private double numHashes = KMV.DEFAULT_K;

  @Option(names = "--no-index", description = "Skip indexing")
  private boolean noIndex = false;

  public static void main(String[] args) {
    CliTool.run(args, new IndexCorrelationBenchmark());
  }

  @Override
  public void execute() throws IOException {
    ColumnStoreMetadata storeMetadata = CreateColumnStore.readMetadata(inputPath);
    BytesBytesHashtable columnStore = new BytesBytesHashtable(storeMetadata.dbType, inputPath);

    QueryStats querySample = selectQueriesRandomly(storeMetadata, numQueries);

    // builds and closes index
    if (!noIndex) {
      buildIndex(columnStore, outputPath, sketchType, numHashes, querySample);
    }

    // Execute queries against the index
    runQueries(columnStore, querySample);

    columnStore.close();
  }

  public void buildIndex(
      BytesBytesHashtable columnStore,
      String outputPath,
      SketchType sketchType,
      double numHashes,
      QueryStats querySample)
      throws IOException {

    Set<String> queryColumns = querySample.queries;

    System.out.println("Selecting a random sample of columns as queries...");

    // Build index
    SketchIndex index = new SketchIndex(outputPath, sketchType, numHashes);

    System.out.println("Indexing all columns...");

    Iterator<KV<byte[], byte[]>> it = columnStore.iterator();
    int i = 0;
    while (it.hasNext()) {

      KV<byte[], byte[]> kv = it.next();
      String key = new String(kv.getKey());
      ColumnPair columnPair = KRYO.unserializeObject(kv.getValue());

      if (!queryColumns.contains(key)) {
        index.index(key, columnPair);
      }

      i++;
      if (i % (querySample.totalColumns / 50) == 0) {
        final double percent = i / (double) querySample.totalColumns * 100;
        System.out.printf("Indexed %d columns (%.2f%%)\n", i, percent);
      }
    }
    final double percent = i / (double) querySample.totalColumns * 100;
    System.out.printf("Indexed %d columns (%.2f%%)\n", i, percent);

    // close index to force flushing data to disk
    index.close();
  }

  private void runQueries(BytesBytesHashtable columnStore, QueryStats querySample)
      throws IOException {

    // re-opens index
    SketchIndex index = new SketchIndex(outputPath, sketchType, numHashes);

    FileWriter csv = new FileWriter(Paths.get(outputPath, "query-times.csv").toFile());
    csv.write("qid, k, time, qcard\n");

    System.out.println("Running queries against the index...");
    Set<String> queryColumns = querySample.queries;
    //    DoubleList times = new DoubleArrayList();
    for (String query : queryColumns) {
      byte[] columnPairBytes = columnStore.get(query.getBytes());
      ColumnPair columnPair = KRYO.unserializeObject(columnPairBytes);
      int k = 100;

      long start = System.nanoTime();
      List<Hit> hits = index.search(columnPair, k);
      long elapsedTime = System.nanoTime() - start;

      final double timeMs = elapsedTime / 1000000d;
      final String csvLine =
          String.format("%s,%f,%.3f,%d\n", query, numHashes, timeMs, columnPair.keyValues.size());
      csv.write(csvLine);
    }

    index.close();
    csv.close();
    System.out.println("Done.");
  }

  private QueryStats selectQueriesRandomly(ColumnStoreMetadata storeMetadata, int sampleSize) {
    List<String> queries = new ArrayList<>();
    Random random = new Random(0);
    int seen = 0;
    for (Set<String> columnSet : storeMetadata.columnSets) {
      for (String column : columnSet) {
        if (queries.size() < sampleSize) {
          queries.add(column);
        } else {
          int index = random.nextInt(seen + 1);
          if (index < sampleSize) {
            queries.set(index, column);
          }
        }
        seen++;
      }
    }
    System.out.printf("Total columns: %d\tQueries selected: %d\n", seen, queries.size());
    QueryStats stats = new QueryStats();
    stats.queries = new HashSet<>(queries);
    stats.totalColumns = seen;
    return stats;
  }

  static class QueryStats {

    int totalColumns;
    Set<String> queries;
  }
}
