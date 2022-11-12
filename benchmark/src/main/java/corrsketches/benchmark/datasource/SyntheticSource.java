package corrsketches.benchmark.datasource;

import static java.lang.Math.log;
import static java.lang.Math.max;

import corrsketches.Column;
import corrsketches.ColumnType;
import corrsketches.benchmark.ColumnPair;
import corrsketches.benchmark.pairwise.ColumnCombination;
import corrsketches.benchmark.pairwise.SyntheticColumnCombination;
import corrsketches.statistics.Stats;
import corrsketches.util.RandomArrays;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;

public class SyntheticSource {

  public static List<ColumnCombination> createColumnCombinations(int numberOfColumns, Random rng) {
    double[] corrs = RandomArrays.randDoubleUniform(numberOfColumns, rng);
    // double[] jcs = RandomArrays.randDoubleUniform(numberOfColumns, rng);

    List<ColumnCombination> combinations = new ArrayList<>();
    for (int i = 0; i < numberOfColumns; i++) {
      float rho = Math.round(corrs[i] * 1000.0) / 1000f;
      // float jc = Math.round(jcs[i] * 100.0) / 100f;

      for (var dataPairType : PairDataType.values()) {
        for (var keyDist : KeyDistribution.values()) {
          combinations.add(
              new BivariateNormalColumnCombination(
                  rho, rng.nextInt(), new PairTypeParams(keyDist, dataPairType)));
        }
      }
    }
    return combinations;
  }

  private static Pair generateColumns(int maxRows, int seed, float rho, PairTypeParams params) {
    // RandomGenerator rng = new JDKRandomGenerator(seed);
    RandomGenerator rng = new Well19937c(seed);
    String[] K = generateRandomKeys(maxRows, params.keyDistribution, rng);
    double[][] sampled = sampleBivariateNormal(maxRows, rho, rng);
    double[] X = sampled[0];
    double[] Y = sampled[1];

    String datasetId =
        "sbn_r="
            + rho
            + "_xtype="
            + params.typeX
            + "_ytype="
            + params.typeY
            + "_keydist="
            + params.keyDistribution;

    String keyName = "K" + seed;
    List<String> keyValues = Arrays.asList(K);
    String columnNameX = "X" + seed;
    String columnNameY = "Y" + seed;

    final int numberOfBins = (int) max(2, log(maxRows));

    if (params.typeX == ColumnType.CATEGORICAL) {
      X = Column.castToDoubleArray(Stats.binEqualWidth(X, numberOfBins));
    }
    if (params.typeY == ColumnType.CATEGORICAL) {
      Y = Column.castToDoubleArray(Stats.binEqualWidth(Y, numberOfBins));
    }

    ColumnPair xcp = new ColumnPair(datasetId, keyName, keyValues, columnNameX, params.typeX, X);
    ColumnPair ycp = new ColumnPair(datasetId, keyName, keyValues, columnNameY, params.typeY, Y);

    return new Pair(xcp, ycp);
  }

  private static String[] generateRandomKeys(
      int length, KeyDistribution distribution, RandomGenerator rng) {
    final int[] randomIds;
    if (distribution == KeyDistribution.UNIFORM) {
      randomIds = randIntUniform(length, rng);
    } else if (distribution == KeyDistribution.ZIPF_1) {
      randomIds = randIntZipf(length, 1, rng);
    } else if (distribution == KeyDistribution.ZIPF_1_5) {
      randomIds = randIntZipf(length, 1.5, rng);
    } else {
      throw new UnsupportedOperationException();
    }
    String[] K = new String[length];
    for (int i = 0; i < length; i++) {
      K[i] = String.valueOf(randomIds[i]);
    }
    return K;
  }

  /**
   * Generates an array of size {@code length} with random values that follow a uniform
   * distribution.
   *
   * @param length the size of the output vector
   * @param rng the random number generator
   * @return a vector with random data uniformly distributed
   */
  public static int[] randIntUniform(int length, final RandomGenerator rng) {
    final int[] data = new int[length];
    for (int i = 0; i < data.length; i++) {
      data[i] = rng.nextInt();
    }
    return data;
  }

  /**
   * Generates an array of size {@code length} with random values that follow a Zip distribution.
   *
   * @param length the size of the output vector
   * @param rng the random number generator
   * @return a Zipfian random vector
   */
  public static int[] randIntZipf(int length, double exponent, final RandomGenerator rng) {
    ZipfDistribution zipfd = new ZipfDistribution(rng, length, exponent);
    int[] samples = new int[length];
    for (int i = 0; i < length; i++) {
      samples[i] = zipfd.sample();
    }
    return samples;
  }

  /**
   * Samples data points from a bivariate normal distribution with the given correlation.
   *
   * @param sampleSize the number of samples to generate
   * @param correlation the desired level of correlation (Pearson's correlation)
   * @param rng the random number generator
   * @return a matrix of dimensions (2, sampleSize) containing samples of the
   */
  private static double[][] sampleBivariateNormal(
      int sampleSize, double correlation, RandomGenerator rng) {
    // Correlation equal to +1 or -1 leads to a singular matrix, which causes a
    // SingularMatrixException,
    // so we 'round' down/up them to their closest values.
    final double r;
    if (correlation == 1) {
      r = Math.nextDown(1);
    } else if (correlation == -1) {
      r = Math.nextUp(-1);
    } else {
      r = correlation;
    }
    double[] means = new double[] {0, 0};
    double[][] covariances = new double[][] {new double[] {1, r}, new double[] {r, 1}};
    var mnd = new MultivariateNormalDistribution(rng, means, covariances);
    return transposeMatrix(mnd.sample(sampleSize));
  }

  /**
   * Transposes a matrix.
   *
   * @param data a (m,n)-dimensional matrix
   * @return the transpose matrix of the input with dimensions (n, m)
   */
  public static double[][] transposeMatrix(double[][] data) {
    final int n = data[0].length;
    final int m = data.length;
    double[][] transposed = new double[n][m];
    for (int i = 0; i < m; i++) {
      for (int j = 0; j < n; j++) {
        transposed[j][i] = data[i][j];
      }
    }
    return transposed;
  }

  public static class PairTypeParams {

    public final KeyDistribution keyDistribution;
    public final ColumnType typeX;
    public final ColumnType typeY;

    public PairTypeParams(KeyDistribution keyDistribution, PairDataType pairType) {
      this.keyDistribution = keyDistribution;
      if (pairType == PairDataType.DISCRETE) {
        typeX = ColumnType.CATEGORICAL;
        typeY = ColumnType.CATEGORICAL;
      } else if (pairType == PairDataType.NUMERICAL) {
        typeX = ColumnType.NUMERICAL;
        typeY = ColumnType.NUMERICAL;
      } else {
        typeX = ColumnType.CATEGORICAL;
        typeY = ColumnType.NUMERICAL;
      }
    }
  }

  private enum PairDataType {
    NUMERICAL,
    DISCRETE,
    MIXED
  }

  public enum KeyDistribution {
    UNIFORM,
    ZIPF_1,
    ZIPF_1_5
  }

  private static class Pair {

    public final ColumnPair y;
    public final ColumnPair x;

    public Pair(ColumnPair x, ColumnPair y) {
      this.x = x;
      this.y = y;
    }
  }

  public static class BivariateNormalColumnCombination implements SyntheticColumnCombination {

    private final int MAX_ROWS = 10000;
    public final PairTypeParams pairTypeParams;
    public final float correlation;
    public final int seed;

    private Pair pair;

    public BivariateNormalColumnCombination(
        float correlation, int seed, PairTypeParams pairTypeParams) {
      this.correlation = correlation;
      this.seed = seed;
      this.pairTypeParams = pairTypeParams;
    }

    public ColumnPair getX() {
      if (this.pair == null) {
        this.pair = generateColumns(MAX_ROWS, seed, correlation, pairTypeParams);
      }
      return pair.x;
    }

    public ColumnPair getY() {
      if (this.pair == null) {
        this.pair = generateColumns(MAX_ROWS, seed, correlation, pairTypeParams);
      }
      return pair.y;
    }

    @Override
    public float getCorrelation() {
      return correlation;
    }

    @Override
    public String getKeyDistribution() {
      return pairTypeParams.keyDistribution.toString();
    }
  }
}
