package spark;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.checkerframework.checker.nullness.qual.Nullable;
import scala.Tuple2;
import benchmark.BenchmarkUtils;
import benchmark.BenchmarkUtils.Result;
import benchmark.ColumnPair;
import sketches.correlation.Sketches.Type;
import sketches.kmv.KMV;
import utils.CliTool;

@Command(
    name = ComputePairwiseCorrelationJoins.JOB_NAME,
    description = "Compute correlation after joins of each pair of categorical-numeric column")
public class ComputePairwiseCorrelationJoins extends CliTool implements Serializable {

  public static final String JOB_NAME = "ComputePairwiseCorrelationJoins";

  @Required
  @Option(name = "--input-path", description = "Folder containing CSV files")
  private String inputPath;

  @Required
  @Option(name = "--output-path", description = "Output path for results file")
  private String outputPath;

  @Option(name = "--sketch-type", description = "The type sketch to be used")
  private Type sketchType = Type.KMV;

  @Required
  @Option(name = "--num-hashes", description = "Number of hashes per sketch")
  private double numHashes = KMV.DEFAULT_K;

  @Option(name = "--min-rows", description = "Minimum number of rows to consider table")
  int minRows = 1;

  @Required
  @Option(
      name = "--spark-conf",
      description = "Semi-collon separated key-value pair to set into the SparkConf")
  private String sparkConf;

  public static void main(String[] args) {
    CliTool.run(args, new ComputePairwiseCorrelationJoins());
  }

  @Override
  public void execute() {
    SparkConf conf = SparkUtils.createSparkConf(JOB_NAME, this.sparkConf, ColumnPair.class);
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaPairRDD<String, PortableDataStream> fileRDD = sc.binaryFiles(this.inputPath);

    JavaRDD<ColumnPair> columnPairRDD =
        fileRDD.flatMap(
            (Tuple2<String, PortableDataStream> kv) -> {
              String fileName = kv._1;
              DataInputStream is = kv._2.open();
              try {
                String datasetName = Paths.get(fileName).getFileName().toString();
                Iterator<ColumnPair> it = BenchmarkUtils.readColumnPairs(datasetName, is, minRows);
                List<ColumnPair> list = new ArrayList<>();
                while (it.hasNext()) {
                  list.add(it.next());
                }
                return list;
              } finally {
                closeQuietly(is);
              }
            });
    columnPairRDD.cache();

    JavaPairRDD<ColumnPair, ColumnPair> columnPairsCombs =
        columnPairRDD
            .cartesian(columnPairRDD)
            .filter(kv -> kv._1.toString().compareTo(kv._2.toString()) < 0);
    JavaRDD<String> resultsRDD =
        columnPairsCombs
            .cache()
            .flatMap(
                (kv) -> {
                  ColumnPair query = kv._1;
                  ColumnPair column = kv._2;
                  Result result = BenchmarkUtils.computeStatistics(query, column, sketchType, numHashes);
                  return result == null ? Collections.emptyList() : Arrays.asList(result.csvLine());
                });

    resultsRDD.saveAsTextFile(outputPath);
    sc.stop();

    System.out.println(getClass().getSimpleName() + " finished successfully.");
  }

  public static void closeQuietly(@Nullable InputStream inputStream) {
    try {
      close(inputStream, true);
    } catch (IOException impossible) {
      throw new AssertionError(impossible);
    }
  }

  public static void close(@Nullable Closeable closeable, boolean swallowIOException)
      throws IOException {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (IOException e) {
      if (swallowIOException) {
        System.err.println("WARNING: IOException thrown while closing Closeable.");
        e.printStackTrace();
      } else {
        throw e;
      }
    }
  }
}
