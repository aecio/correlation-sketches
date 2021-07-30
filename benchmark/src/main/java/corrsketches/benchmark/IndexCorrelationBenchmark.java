package corrsketches.benchmark;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.Builder;
import corrsketches.SketchType;
import corrsketches.SketchType.GKMVOptions;
import corrsketches.SketchType.KMVOptions;
import corrsketches.SketchType.SketchOptions;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.CreateColumnStore.ColumnStoreMetadata;
import corrsketches.benchmark.JoinAggregation.NumericJoinAggregation;
import corrsketches.benchmark.index.Hit;
import corrsketches.benchmark.index.Hit.CorrelationSketchReranker;
import corrsketches.benchmark.index.Hit.RerankStrategy;
import corrsketches.benchmark.index.QCRISketchIndex;
import corrsketches.benchmark.index.QCRSketchIndex;
import corrsketches.benchmark.index.SketchIndex;
import corrsketches.benchmark.utils.EvalMetrics;
import corrsketches.benchmark.utils.Sets;
import corrsketches.correlation.PearsonCorrelation;
import corrsketches.statistics.Stats;
import hashtabledb.BytesBytesHashtable;
import hashtabledb.KV;
import hashtabledb.Kryos;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
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

  @Option(
      names = "--input-path",
      required = true,
      description = "Folder containing key-value column store")
  String inputPath;

  @Option(names = "--output-path", required = true, description = "Output path for results file")
  String outputPath;

  @Option(names = "--params", required = true, description = "Benchmark parameters")
  String params;

  @Option(names = "--num-queries", description = "The number of queries to be run")
  int numQueries = 1000;

  @Option(names = "--cores", description = "The number of CPU cores to be used")
  int cores = 8;
  //    int cores = Runtime.getRuntime().availableProcessors();

  @Option(
      names = "--aggregate",
      description = "The aggregate functions to be used by correlation sketches")
  AggregateFunction aggregate = AggregateFunction.FIRST;

  @Option(
      names = "--runs",
      description = "The number of runs to execute for the timeQueries command")
  int runs = 5;

  public static void main(String[] args) {
    System.exit(new CommandLine(new IndexCorrelationBenchmark()).execute(args));
  }

  @Command(name = "buildIndex")
  public void buildIndex() throws Exception {
    ColumnStoreMetadata storeMetadata = CreateColumnStore.readMetadata(inputPath);
    final boolean readonly = true;
    BytesBytesHashtable columnStore =
        new BytesBytesHashtable(storeMetadata.dbType, inputPath, readonly);

    QueryStats querySample = selectQueriesRandomly(storeMetadata, numQueries);

    final Map<String, SketchIndex> indexes = createIndexes(this.params);
    final Runnable task =
        () ->
            indexes.entrySet().stream()
                .parallel()
                .forEach(
                    (var each) -> {
                      try {
                        buildIndex(columnStore, each.getKey(), each.getValue(), querySample);
                      } catch (IOException e) {
                        e.printStackTrace();
                      }
                    });
    parallelExecute(task);
    writeQuerySample(querySample, outputPath);
    columnStore.close();
    System.out.println("Done.");
  }

  public Map<String, SketchIndex> createIndexes(String params) throws IOException {
    final List<BenchmarkParams> benchmarkParams = BenchmarkParams.parse(params);
    final Map<String, SketchIndex> indexes = new HashMap();
    for (var p : benchmarkParams) {
      final String indexPath = indexPath(outputPath, p.indexType, p.sketchOptions);
      if (!indexes.containsKey(indexPath)) {
        if (Files.exists(Paths.get(indexPath))) {
          System.out.printf("Index directory already exits, skipping (%s).\n", indexPath);
        } else {
          indexes.put(indexPath, openSketchIndex(outputPath, p, false));
        }
      }
    }
    return indexes;
  }

  public void buildIndex(
      BytesBytesHashtable columnStore, String indexName, SketchIndex index, QueryStats querySample)
      throws IOException {

    System.out.println("Indexing all columns...");

    Set<String> queryColumns = querySample.queries;
    Iterator<KV<byte[], byte[]>> it = columnStore.iterator();
    int i = 0;
    printProgress(querySample, indexName, i);
    while (it.hasNext()) {

      KV<byte[], byte[]> kv = it.next();
      String key = new String(kv.getKey());
      ColumnPair columnPair = KRYO.unserializeObject(kv.getValue());

      if (!queryColumns.contains(key)) {
        index.index(key, columnPair);
      }

      i++;
      if (i % (querySample.totalColumns / 25) == 0) {
        printProgress(querySample, indexName, i);
      }
    }
    printProgress(querySample, indexName, i);

    // close index to force flushing data to disk
    index.close();
  }

  private static void printProgress(QueryStats querySample, String indexName, int i) {
    final double percent = i / (double) querySample.totalColumns * 100;
    System.out.printf("[%s] Indexed %d columns (%.2f%%)\n", indexName, i, percent);
  }

  @Command(name = "runQueries")
  public void runQueriesBenchmark() throws Exception {
    ColumnStoreMetadata storeMetadata = CreateColumnStore.readMetadata(inputPath);
    BytesBytesHashtable columnStore = new BytesBytesHashtable(storeMetadata.dbType, inputPath);
    QueryStats querySample = readQuerySample(outputPath);
    runQueries(columnStore, querySample, BenchmarkParams.parse(this.params));
    columnStore.close();
  }

  /** Execute queries against the index */
  private void runQueries(
      BytesBytesHashtable columnStore, QueryStats querySample, List<BenchmarkParams> params)
      throws Exception {

    // opens the index
    List<SketchIndex> indexes = new ArrayList<>(params.size());
    for (var p : params) {
      indexes.add(openSketchIndex(outputPath, p, true));
    }

    FileWriter metricsCsv = new FileWriter(Paths.get(outputPath, "query-metrics.csv").toFile());
    metricsCsv.write(
        "qid, params, qcard, n_hits, ndgc@5, ndgc@10, ndcg@50, recall_r>0.25, recall_r>0.50, recall_r>0.75, avg_jc\n");

    System.out.println("Running queries against the index...");
    Set<String> queryIds = querySample.queries;

    int count = 0;
    for (String qid : queryIds) {

      ColumnPair queryColumnPair = readColumnPair(columnStore, qid);
      final int queryCard = queryColumnPair.keyValues.size();

      var allHitLists = new ArrayList<List<Hit>>();
      for (int paramIdx = 0; paramIdx < params.size(); paramIdx++) {
        SketchIndex index = indexes.get(paramIdx);
        final int topK = params.get(paramIdx).topK;
        List<Hit> hits = index.search(queryColumnPair, topK);
        allHitLists.add(hits);
      }

      List<GroundTruth> groundTruth = computeGroundTruth(columnStore, queryColumnPair, allHitLists);

      for (int paramIdx = 0; paramIdx < params.size(); paramIdx++) {
        List<Hit> hits = allHitLists.get(paramIdx);
        Scores scores = computeRankingScores(hits, groundTruth, params.get(paramIdx).params);
        scores.avgJC = avgJaccardContainment(queryCard, hits, groundTruth);

        String csvLine =
            String.format(
                "%s,%s,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n",
                qid,
                scores.params,
                queryCard,
                hits.size(),
                scores.ndcg5,
                scores.ndcg10,
                scores.ndcg50,
                scores.recallR025,
                scores.recallR050,
                scores.recallR075,
                scores.avgJC);
        metricsCsv.write(csvLine);
        metricsCsv.flush();
      }

      count++;
      System.out.printf(
          "Processed %d queries (%.3f%%)\n", count, 100 * count / (double) queryIds.size());
    }

    for (var index : indexes) {
      index.close();
    }
    metricsCsv.close();

    System.out.println("Done.");
  }

  private Scores computeRankingScores(
      List<Hit> hits, List<GroundTruth> groundTruth, String sketchParams) {

    final Scores scores = new Scores();
    scores.params = sketchParams;

    if (hits.isEmpty()) {
      return scores;
    }

    Map<String, Double> relevanceMap = new HashMap<>();
    for (var gt : groundTruth) {
      relevanceMap.put(gt.hitId, Math.abs(gt.corr_actual));
    }

    final EvalMetrics metrics = new EvalMetrics(relevanceMap);
    scores.recallR025 = metrics.recall(hits, 0.25);
    scores.recallR050 = metrics.recall(hits, 0.50);
    scores.recallR075 = metrics.recall(hits, 0.75);
    scores.ndcg5 = metrics.ndgc(hits, 5);
    scores.ndcg10 = metrics.ndgc(hits, 10);
    scores.ndcg50 = metrics.ndgc(hits, 50);

    return scores;
  }

  private double avgJaccardContainment(int queryCard, List<Hit> hits, List<GroundTruth> groundTruth) {
    Map<String, Integer> overlapMap = new HashMap<>();
    for (var gt : groundTruth) {
      overlapMap.put(gt.hitId, gt.overlap_qc_actual);
    }
    double[] jcs = new double[hits.size()];
    for(int i = 0; i < hits.size(); i++) {
      jcs[i] = overlapMap.get(hits.get(i).id) / (double) queryCard;
    }
    return Stats.mean(jcs);
  }

  @Command(name = "timeQueries")
  public void timeQueriesBenchmark() throws Exception {
    ColumnStoreMetadata storeMetadata = CreateColumnStore.readMetadata(inputPath);
    BytesBytesHashtable columnStore = new BytesBytesHashtable(storeMetadata.dbType, inputPath);
    QueryStats querySample = readQuerySample(outputPath);
    timeQueries(columnStore, querySample, BenchmarkParams.parse(this.params));
    columnStore.close();
  }

  private void timeQueries(
      BytesBytesHashtable columnStore, QueryStats querySample, List<BenchmarkParams> params)
      throws Exception {

    // opens the index
    List<SketchIndex> indexes = new ArrayList<>(params.size());
    for (var p : params) {
      indexes.add(openSketchIndex(outputPath, p, true));
    }

    FileWriter csvHits = new FileWriter(Paths.get(outputPath, "query-times.csv").toFile());
    csvHits.write("qid, params, run, qcard, n_hits, time\n");

    System.out.println("Running queries against the index...");
    Set<String> queryIds = querySample.queries;
    int count = 0;
    for (String qid : queryIds) {

      ColumnPair queryColumnPair = readColumnPair(columnStore, qid);
      final int queryCard = queryColumnPair.keyValues.size();

      for (int paramIdx = 0; paramIdx < params.size(); paramIdx++) {
        var index = indexes.get(paramIdx);
        final String sketchParams = params.get(paramIdx).params;
        final int topK = params.get(paramIdx).topK;

        for (int i = 0; i < this.runs; i++) {
          // measure search time
          long start = System.nanoTime();
          List<Hit> hits = index.search(queryColumnPair, topK);
          final int timeMs = (int) ((System.nanoTime() - start) / 1000000d);
          // log results
          csvHits.write(
              String.format(
                  "%s,%s,%d,%d,%d,%d\n", qid, sketchParams, i, queryCard, hits.size(), timeMs));
        }
      }
      count++;
      System.out.printf(
          "Processed %d queries (%.3f%%)\n", count, 100 * count / (double) queryIds.size());
    }

    for (var index : indexes) {
      index.close();
    }
    csvHits.close();

    System.out.println("Done.");
  }

  private static ColumnPair readColumnPair(BytesBytesHashtable columnStore, String query) {
    byte[] columnPairBytes = columnStore.get(query.getBytes());
    return KRYO.unserializeObject(columnPairBytes);
  }

  private List<GroundTruth> computeGroundTruth(
      BytesBytesHashtable columnStore, ColumnPair queryColumnPair, List<List<Hit>> allHitLists)
      throws ExecutionException, InterruptedException {

    List<String> allHitIds =
        allHitLists.stream()
            .flatMap(List::stream)
            .map(hit -> hit.id)
            .distinct()
            .collect(Collectors.toList());

    List<AggregateFunction> aggregateFunctions = Arrays.asList(aggregate);

    return parallelExecute(
        () ->
            allHitIds.stream()
                .parallel()
                .map(
                    (String hitId) -> {
                      ColumnPair hitColumnPair = getColumnPair(cache, columnStore, hitId);

                      HashSet<String> xKeys = new HashSet<>(queryColumnPair.keyValues);
                      HashSet<String> yKeys = new HashSet<>(hitColumnPair.keyValues);

                      var gt = new GroundTruth();
                      gt.hitId = hitId;
                      gt.card_q_actual = xKeys.size();
                      gt.card_c_actual = yKeys.size();
                      gt.overlap_qc_actual = Sets.intersectionSize(xKeys, yKeys);
                      gt.corr_actual =
                          computeCorrelation(
                              queryColumnPair, aggregateFunctions, hitId, hitColumnPair);
                      return gt;
                    })
                .collect(Collectors.toList()));
  }

  private static double computeCorrelation(
      ColumnPair queryColumnPair,
      List<AggregateFunction> functions,
      String hitId,
      ColumnPair hitColumnPair) {
    double correlation;
    MetricsResult results = new MetricsResult();
    List<MetricsResult> metricsResults =
        computeCorrelationsAfterJoin(queryColumnPair, hitColumnPair, functions, results);
    if (!metricsResults.isEmpty()) {
      correlation = metricsResults.get(0).corr_rp_actual;
    } else {
      System.out.printf(
          "WARN: no correlation computed for query.id=[%s] hit.id=[%s] join size=[%d]\n",
          queryColumnPair.id(), hitId, queryColumnPair.keyValues.size());
      correlation = 0;
    }
    return correlation;
  }

  public static List<MetricsResult> computeCorrelationsAfterJoin(
      ColumnPair columnA,
      ColumnPair columnB,
      List<AggregateFunction> functions,
      MetricsResult result) {

    List<NumericJoinAggregation> joins =
        JoinAggregation.numericJoinAggregate(columnA, columnB, functions);

    List<MetricsResult> results = new ArrayList<>(functions.size());
    for (NumericJoinAggregation join : joins) {
      double[] joinedA = join.valuesA;
      double[] joinedB = join.valuesB;
      // correlation is defined only for vectors of length at least two
      MetricsResult r = result.clone();
      r.aggregate = join.aggregate;
      int minimumIntersection = 3; // TODO: what value to use here?
      if (joinedA.length < minimumIntersection) {
        r.corr_rp_actual = Double.NaN;
      } else {
        r.corr_rp_actual = PearsonCorrelation.coefficient(joinedA, joinedB);
      }
      results.add(r);
    }

    return results;
  }

  private void parallelExecute(Runnable task) throws InterruptedException, ExecutionException {
    ForkJoinPool forkJoinPool = new ForkJoinPool(this.cores);
    forkJoinPool.submit(task).get();
  }

  private <T> T parallelExecute(Callable<T> task) throws InterruptedException, ExecutionException {
    ForkJoinPool forkJoinPool = new ForkJoinPool(cores);
    return forkJoinPool.submit(task).get();
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
    System.out.println("Selecting a random sample of columns as queries...");
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
    System.out.println();
    System.out.printf("Total columns: %d\tQueries selected: %d\n", seen, queries.size());
    QueryStats stats = new QueryStats();
    stats.queries = new HashSet<>(queries);
    stats.totalColumns = seen;
    return stats;
  }

  private SketchIndex openSketchIndex(String outputPath, BenchmarkParams params, boolean readonly)
      throws IOException {

    SketchType sketchType = params.sketchOptions.type;
    Builder builder = CorrelationSketch.builder();
    builder.aggregateFunction(aggregate);
    switch (params.sketchOptions.type) {
      case KMV:
        builder.sketchType(sketchType, ((KMVOptions) params.sketchOptions).k);
        break;
      case GKMV:
        builder.sketchType(sketchType, ((GKMVOptions) params.sketchOptions).t);
        break;
      default:
        throw new IllegalArgumentException("Unsupported sketch type: " + params.sketchOptions.type);
    }

    String indexPath = indexPath(outputPath, params.indexType, params.sketchOptions);
    IndexType indexType = params.indexType;
    try {
      switch (indexType) {
        case STD:
          return new SketchIndex(indexPath, builder, params.sortBy, readonly);
        case QCR:
          return new QCRSketchIndex(indexPath, builder, params.sortBy, readonly);
        case QCRI:
          return new QCRISketchIndex(indexPath, builder, params.sortBy, readonly);
        default:
          throw new IllegalArgumentException("Undefined index type: " + indexType);
      }
    } finally {
      System.out.printf("Opened index of type (%s) at: %s\n", indexType, indexPath);
    }
  }

  private static String indexPath(
      String outputPath, IndexType indexType, SketchOptions sketchOptions) {
    return Paths.get(outputPath, "indexes", indexType.toString() + ":" + sketchOptions.name())
        .toString();
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

  static class Scores {

    public String params;
    public double ndcg5;
    public double ndcg10;
    public double recallR050;
    public double recallR075;
    public double recallR025;
    public double ndcg50;
    public double avgJC;
  }

  static class GroundTruth {

    public String hitId;
    public int card_q_actual;
    public int card_c_actual;
    public int overlap_qc_actual;
    public double corr_actual;
  }

  static class QueryStats {

    int totalColumns;
    Set<String> queries;
  }

  public static class BenchmarkParams {

    public final String params;
    public final IndexType indexType;
    public final SortBy sortBy;
    public final int topK;
    public final SketchOptions sketchOptions;

    public BenchmarkParams(
        String params, IndexType indexType, SortBy sortBy, int topK, SketchOptions sketchOptions) {
      this.params = params;
      this.indexType = indexType;
      this.sortBy = sortBy;
      this.topK = topK;
      this.sketchOptions = sketchOptions;
    }

    public static List<BenchmarkParams> parse(String params) {
      String[] values = params.split(",");
      List<BenchmarkParams> result = new ArrayList<>();
      for (String value : values) {
        result.add(parseValue(value.trim()));
      }
      if (result.isEmpty()) {
        throw new IllegalArgumentException(
            String.format("[%s] does not have any valid sketch parameters", params));
      }
      return result;
    }

    private static BenchmarkParams parseValue(String params) {
      String[] values = params.split(":");
      final int argc = 5;
      if (values.length == argc) {
        int i = 0;
        IndexType indexType = IndexType.valueOf(values[i].trim());
        i++;
        SortBy sortBy = SortBy.valueOf(values[i].trim());
        i++;
        int topK = Integer.valueOf(values[i].trim());
        i++;
        SketchType sketchType = SketchType.valueOf(values[i].trim());
        i++;
        SketchOptions options = SketchType.parseOptions(sketchType, values[i]);
        return new BenchmarkParams(params, indexType, sortBy, topK, options);
      }
      throw new IllegalArgumentException(
          String.format("[%s] is not a valid parameter. Must have %d parameters", params, argc));
    }

    @Override
    public String toString() {
      return params;
    }
  }

  public enum IndexType {
    QCR,
    QCRI,
    STD,
  }

  public enum SortBy {
    KEY(null),
    CSK(new CorrelationSketchReranker());

    public RerankStrategy reranker;

    SortBy(RerankStrategy reranker) {
      this.reranker = reranker;
    }
  }
  // public static class IndexOptions {
  //
  //   IndexType indexType = IndexType.QCR;
  //   SortBy sortBy = SortBy.CSK;
  //   int topK = 100;
  //
  //   public IndexOptions(SortBy type) {
  //     sortBy = type;
  //   }
  //
  //   static final IndexOptions parse(String params) {
  //     try {
  //       SortBy type = SortBy.valueOf(params);
  //       return new IndexOptions(type);
  //     } catch (Exception e) {
  //       throw new IllegalArgumentException("Unsupported sketch type: " + params);
  //     }
  //   }
  // }
}
