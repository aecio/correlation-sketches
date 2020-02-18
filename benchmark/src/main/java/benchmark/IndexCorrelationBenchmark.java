package benchmark;

import benchmark.CreateColumnStore.ColumnStoreMetadata;
import benchmark.index.SketchIndex;
import benchmark.index.SketchIndex.Hit;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import hashtabledb.BytesBytesHashtable;
import hashtabledb.KV;
import hashtabledb.Kryos;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Random;
import java.util.Set;
import sketches.correlation.SketchType;
import sketches.kmv.KMV;
import spark.ComputePairwiseCorrelationJoins;
import utils.CliTool;

@Command(
    name = IndexCorrelationBenchmark.JOB_NAME,
    description = "Creates a Lucene index of tables")
public class IndexCorrelationBenchmark extends CliTool implements Serializable {

  public static final String JOB_NAME = "IndexCorrelationBenchmark";

  public static final Kryos<ColumnPair> KRYO = new Kryos(ColumnPair.class);

  @Required
  @Option(name = "--input-path", description = "Folder containing key-value column store")
  String inputPath;

  @Required
  @Option(name = "--output-path", description = "Output path for results file")
  String outputPath;

//  @Option(name = "--estimator", description = "The correlation estimator to be used")
//  CorrelationType estimator = CorrelationType.PEARSONS;

  @Option(name = "--sketch-type", description = "The type sketch to be used")
  SketchType sketchType = SketchType.KMV;

  @Option(name = "--num-queries", description = "The numbers of queries to be run")
  int numQueries = 100;

  @Required
  @Option(name = "--num-hashes", description = "Number of hashes per sketch")
  private double numHashes = KMV.DEFAULT_K;

  public static void main(String[] args) {
    CliTool.run(args, new ComputePairwiseCorrelationJoins());
  }

  @Override
  public void execute() throws IOException {

    ColumnStoreMetadata storeMetadata = CreateColumnStore.readMetadata(inputPath);
    BytesBytesHashtable columnStore = new BytesBytesHashtable(storeMetadata.dbType, inputPath);

    System.out.println("Selecting a random sample of columns as queries...");

    Set<String> queryColumns = new HashSet<>(selectQueriesRandomly(storeMetadata, numQueries));

    // Build index
    String indexPath = outputPath;
    SketchIndex index = new SketchIndex(indexPath, sketchType, numHashes);

    System.out.println("Indexing all columns...");

    Iterator<KV<byte[], byte[]>> it = columnStore.iterator();
    while (it.hasNext()) {

      KV<byte[], byte[]> kv = it.next();
      String key = new String(kv.getKey());
      ColumnPair columnPair = KRYO.unserializeObject(kv.getValue());

      if (!queryColumns.contains(key)) {
        index.index(key, columnPair);
      }
    }

    // Execute queries against the index
    System.out.println("Running queries against the index...");
    LongList times = new LongArrayList();
    for (String query : queryColumns) {
      byte[] columnPairBytes = columnStore.get(query.getBytes());
      ColumnPair columnPair = KRYO.unserializeObject(columnPairBytes);
      int k = 10;

      long start = System.currentTimeMillis();
      List<Hit> hits = index.search(columnPair, k);
      long elapsedTime = System.currentTimeMillis() - start;

      System.out.printf(" - query-time(ms): %d\n", elapsedTime);
      times.add(elapsedTime);
    }

    LongSummaryStatistics stats = times.stream().mapToLong(Long::longValue).summaryStatistics();
    System.out.printf("query-time-statistics: count=%d, sum=%d, min=%d, average=%f, max=%d\n",
        stats.getCount(),
        stats.getSum(),
        stats.getMin(),
        stats.getAverage(),
        stats.getMax()
    );
  }

  private List<String> selectQueriesRandomly(ColumnStoreMetadata storeMetadata, int sampleSize) {
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
    return queries;
  }
}
