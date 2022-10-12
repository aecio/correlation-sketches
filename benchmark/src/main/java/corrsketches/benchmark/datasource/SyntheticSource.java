package corrsketches.benchmark.datasource;

import static java.lang.Math.log;
import static java.lang.Math.max;

import corrsketches.Column;
import corrsketches.ColumnType;
import corrsketches.benchmark.ColumnPair;
import corrsketches.benchmark.pairwise.ColumnCombination;
import corrsketches.statistics.Stats;
import corrsketches.util.RandomArrays;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;

public class SyntheticSource {

  public static List<ColumnCombination> createColumnCombinations(int numberOfColumns) {
    return createColumnCombinations(numberOfColumns, new Random(1234));
  }

  public static List<ColumnCombination> createColumnCombinations(int numberOfColumns, Random rng) {
    double[] corrs = RandomArrays.randDoubleUniform(numberOfColumns, rng);
    // double[] jcs = RandomArrays.randDoubleUniform(numberOfColumns, rng);

    List<ColumnCombination> combinations = new ArrayList<>();
    for (int i = 0; i < numberOfColumns; i++) {
      float rho = Math.round(corrs[i] * 1000.0) / 1000f;
      // float jc = Math.round(jcs[i] * 100.0) / 100f;
      combinations.add(new SyntheticColumnCombination(rho, rng.nextInt(), PairType.NUMERICAL));
      combinations.add(new SyntheticColumnCombination(rho, rng.nextInt(), PairType.DISCRETE));
      combinations.add(new SyntheticColumnCombination(rho, rng.nextInt(), PairType.MIXED));
    }
    return combinations;
  }

  private static Pair generateColumns(int maxRows, int seed, float rho, PairType pairType) {
    RandomGenerator rng = new JDKRandomGenerator(seed);
    double[][] sampled = sampleBivariateNormal(maxRows, rho, rng);
    double[] X = sampled[0];
    double[] Y = sampled[1];
    String[] K = new String[maxRows];
    for (int j = 0; j < maxRows; j++) {
      K[j] = String.valueOf(rng.nextLong());
    }

    String datasetId = "sbn_r=" + rho + "_type=" + pairType;
    String keyName = "K" + seed;
    List<String> keyValues = Arrays.asList(K);
    String columnNameX = "X" + seed;
    String columnNameY = "Y" + seed;

    final ColumnType typeX;
    final ColumnType typeY;
    final int numberOfBins = (int) max(2, log(maxRows));
    if (pairType == PairType.DISCRETE) {
      typeX = ColumnType.CATEGORICAL;
      typeY = ColumnType.CATEGORICAL;
      X = Column.castToDoubleArray(Stats.binEqualWidth(X, numberOfBins));
      Y = Column.castToDoubleArray(Stats.binEqualWidth(Y, numberOfBins));
    } else if (pairType == PairType.NUMERICAL) {
      typeX = ColumnType.NUMERICAL;
      typeY = ColumnType.NUMERICAL;
    } else {
      typeX = ColumnType.CATEGORICAL;
      typeY = ColumnType.NUMERICAL;
      X = Column.castToDoubleArray(Stats.binEqualWidth(X, numberOfBins));
    }

    ColumnPair xcp = new ColumnPair(datasetId, keyName, keyValues, columnNameX, typeX, X);
    ColumnPair ycp = new ColumnPair(datasetId, keyName, keyValues, columnNameY, typeY, Y);

    return new Pair(xcp, ycp);
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

  private static double[][] sampleBivariateNormal(int sampleSize, double correlation, int seed) {
    return sampleBivariateNormal(sampleSize, correlation, new JDKRandomGenerator(seed));
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

  private enum PairType {
    NUMERICAL,
    DISCRETE,
    MIXED
  }

  private static class Pair {

    public final ColumnPair y;
    public final ColumnPair x;

    public Pair(ColumnPair x, ColumnPair y) {
      this.x = x;
      this.y = y;
    }
  }

  public static class SyntheticColumnCombination implements ColumnCombination {

    private final int MAX_ROWS = 10000;
    public final float correlation;
    public final int seed;
    private PairType pairType;

    private Pair pair;

    public SyntheticColumnCombination(float correlation, int seed, PairType pairType) {
      this.correlation = correlation;
      this.seed = seed;
      this.pairType = pairType;
    }

    public ColumnPair getX() {
      if (this.pair == null) {
        this.pair = generateColumns(MAX_ROWS, seed, correlation, pairType);
      }
      return pair.x;
    }

    public ColumnPair getY() {
      if (this.pair == null) {
        this.pair = generateColumns(MAX_ROWS, seed, correlation, pairType);
      }
      return pair.y;
    }
  }
}
