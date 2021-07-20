package corrsketches.benchmark;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import corrsketches.SketchType;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.CreateColumnStore.ColumnStoreMetadata;
import corrsketches.benchmark.index.Hit;
import corrsketches.benchmark.index.QCRSketchIndex;
import corrsketches.benchmark.index.SketchIndex;
import corrsketches.correlation.PearsonCorrelation;
import corrsketches.kmv.KMV;
import hashtabledb.BytesBytesHashtable;
import hashtabledb.KV;
import hashtabledb.Kryos;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = IndexCorrelationBenchmark.JOB_NAME,
    description = "Creates a Lucene index of tables")
public class IndexCorrelationBenchmark {

  public static final String JOB_NAME = "IndexCorrelationBenchmark";

  public static final Kryos<ColumnPair> KRYO = new Kryos<>(ColumnPair.class);
  Cache<String, ColumnPair> cache = CacheBuilder.newBuilder().softValues().build();

  public enum IndexType {
    QCR,
    STD,
    FULL
  }

  @Option(
      names = "--input-path",
      required = true,
      description = "Folder containing key-value column store")
  String inputPath;

  @Option(names = "--output-path", required = true, description = "Output path for results file")
  String outputPath;

  @Option(names = "--sketch-type", description = "The type of sketch to be used")
  SketchType sketchType = SketchType.KMV;

  @Option(names = "--index-type", description = "The type of index to be used")
  IndexType indexType = IndexType.STD;

  @Option(names = "--num-queries", description = "The numbers of queries to be run")
  int numQueries = 1000;

  @Option(names = "--num-hashes", required = true, description = "Number of hashes per sketch")
  private double numHashes = KMV.DEFAULT_K;

  public static void main(String[] args) {
    System.exit(new CommandLine(new IndexCorrelationBenchmark()).execute(args));
  }

  @Command(name = "buildIndex")
  public void buildIndex() throws IOException {
    ColumnStoreMetadata storeMetadata = CreateColumnStore.readMetadata(inputPath);
    final boolean readonly = true;
    BytesBytesHashtable columnStore =
        new BytesBytesHashtable(storeMetadata.dbType, inputPath, readonly);

    QueryStats querySample = selectQueriesRandomly(storeMetadata, numQueries);
    buildIndex(columnStore, outputPath, sketchType, numHashes, querySample);
    writeQuerySample(querySample, outputPath);
    columnStore.close();
    System.out.println("Done.");
  }

  @Command(name = "runQueries")
  public void queryBenchmark() throws Exception {
    ColumnStoreMetadata storeMetadata = CreateColumnStore.readMetadata(inputPath);
    BytesBytesHashtable columnStore = new BytesBytesHashtable(storeMetadata.dbType, inputPath);

    QueryStats querySample = readQuerySample(outputPath);
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
    SketchIndex index = openSketchIndex(outputPath, indexType, sketchType, numHashes);

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

  /** Execute queries against the index */
  private void runQueries(BytesBytesHashtable columnStore, QueryStats querySample)
      throws IOException, ExecutionException, InterruptedException {

    // opens the index
    SketchIndex index = openSketchIndex(outputPath, indexType, sketchType, numHashes);

    String filename =
        String.format("query-times_%s.csv", indexType.toString().toLowerCase(Locale.ROOT));
    FileWriter csv = new FileWriter(Paths.get(outputPath, filename).toFile());
    csv.write("qid, k, time, qcard, ranking_corr\n");

    System.out.println("Running queries against the index...");
    Set<String> queryColumns = querySample.queries;
    //    DoubleList times = new DoubleArrayList();

    int count = 0;
    for (String query : queryColumns) {
      byte[] columnPairBytes = columnStore.get(query.getBytes());
      ColumnPair queryColumnPair = KRYO.unserializeObject(columnPairBytes);
      int k = 100;

      long start = System.nanoTime();
      List<Hit> hits = index.search(queryColumnPair, k);
      long elapsedTime = System.nanoTime() - start;
      final double timeMs = elapsedTime / 1000000d;

      var pearson = computeActualCorrelations(columnStore, queryColumnPair, hits);
      //      var pearson = metricsResults.get(0).corr_rp_actual;
      //      List<String> collect = metricsResults.stream().map((var m) ->
      // String.valueOf(m.corr_rp_actual))
      //          .collect(Collectors.toList());
      //      String.join(",", collect);
      final String csvLine =
          String.format(
              "%s,%f,%.3f,%d,%.3f\n",
              query, numHashes, timeMs, queryColumnPair.keyValues.size(), pearson);
      csv.write(csvLine);
      csv.flush();
      count++;
      System.out.printf(
          "Processed %d queries (%.3f%%)\n", count, 100 * count / (double) queryColumns.size());
    }

    index.close();
    csv.close();
    System.out.println("Done.");
  }

  private double computeActualCorrelations(
      BytesBytesHashtable columnStore, ColumnPair queryColumnPair, List<Hit> hits)
      throws ExecutionException, InterruptedException {

    List<AggregateFunction> functions = Arrays.asList(AggregateFunction.FIRST);

    MetricsResult results = new MetricsResult();
    results.corr_rp_actual = 0;

    DoubleList actuals = new DoubleArrayList();
    DoubleList scores = new DoubleArrayList();

    //    int cores = Runtime.getRuntime().availableProcessors();
    int cores = 8;
    ForkJoinPool forkJoinPool = new ForkJoinPool(cores);
    forkJoinPool
        .submit(
            () -> {
              hits.stream()
                  .parallel()
                  .forEach(
                      (Hit hit) -> {
                        ColumnPair hitColumnPair = getColumnPair(cache, columnStore, hit.id);

                        List<MetricsResult> metricsResults =
                            BenchmarkUtils.computeCorrelationsAfterJoin(
                                queryColumnPair, hitColumnPair, functions, results);

                        MetricsResult correlations;
                        if (!metricsResults.isEmpty()) {
                          correlations = metricsResults.get(0);
                        } else {
                          //                          System.out.printf(
                          //                              "WARN: no correlation computed for
                          // query.id=[%s] hit.id=[%s] join size=[%d]\n",
                          //                              queryColumnPair.id(), hit.id,
                          // queryColumnPair.keyValues.size());
                          correlations = results;
                        }
                        synchronized (actuals) {
                          actuals.add(Math.abs(correlations.corr_rp_actual));
                          scores.add(hit.score);
                        }
                      });
            })
        .get();
    return PearsonCorrelation.coefficient(actuals.toDoubleArray(), scores.toDoubleArray());
  }

  private static void writeQuerySample(QueryStats querySample, String outputPath)
      throws IOException {
    FileWriter file = new FileWriter(Paths.get(outputPath, "query-sample.txt").toFile());
    file.write(querySample.totalColumns + "\n");
    for (String qid : querySample.queries) {
      file.write(qid);
      file.write('\n');
    }
    file.close();
  }

  private static QueryStats readQuerySample(String outputPath) throws IOException {
    File file = Paths.get(outputPath, "query-sample.txt").toFile();
    BufferedReader fileReader = new BufferedReader(new FileReader(file));
    QueryStats querySample = new QueryStats();
    querySample.totalColumns = Integer.parseInt(fileReader.readLine());
    querySample.queries = new HashSet<>();
    String qid;
    while ((qid = fileReader.readLine()) != null) {
      querySample.queries.add(qid);
    }
    return querySample;
  }

  private static QueryStats selectQueriesRandomly(
      ColumnStoreMetadata storeMetadata, int sampleSize) {
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
    System.out.println("Query examples:");
    for (int i = 0; i < 5 && i < queries.size(); i++) {
      System.out.printf(" [%d] %s", i, queries.get(i));
    }
    System.out.printf("Total columns: %d\tQueries selected: %d\n", seen, queries.size());
    QueryStats stats = new QueryStats();
    stats.queries = new HashSet<>(queries);
    stats.totalColumns = seen;
    return stats;
  }

  private static SketchIndex openSketchIndex(
      String outputPath, IndexType indexType, SketchType sketchType, double numHashes)
      throws IOException {
    String indexPath = indexPath(outputPath, indexType, sketchType, numHashes);
    try {
      switch (indexType) {
        case STD:
          return new SketchIndex(indexPath, sketchType, numHashes);
        case QCR:
          return new QCRSketchIndex(indexPath, sketchType, numHashes);
        default:
          throw new IllegalArgumentException("Undefined index type: " + indexType);
      }
    } finally {
      System.out.printf("Opened index of type (%s) at: %s\n", indexType, outputPath);
    }
  }

  private static String indexPath(
      String outputPath, IndexType indexType, SketchType sketchType, double numHashes) {
    String sketchParams =
        String.format(
            "%s-%s=%.3f",
            indexType.toString().toLowerCase(), sketchType.toString().toLowerCase(), numHashes);
    return Paths.get(outputPath, sketchParams).toString();
  }

  private ColumnPair getColumnPair(
      Cache<String, ColumnPair> cache, BytesBytesHashtable hashtable, String key) {
    ColumnPair cp = cache.getIfPresent(key);
    if (cp == null) {
      byte[] keyBytes = key.getBytes();
      cp = KRYO.unserializeObject(hashtable.get(keyBytes));
      cache.put(key, cp);
    }
    return cp;
  }

  static class QueryStats {

    int totalColumns;
    Set<String> queries;
  }
}
