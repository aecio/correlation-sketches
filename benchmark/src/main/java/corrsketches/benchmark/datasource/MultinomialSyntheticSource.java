package corrsketches.benchmark.datasource;

import static corrsketches.benchmark.utils.LogFactorial.logOfFactorial;

import com.google.common.math.BigIntegerMath;
import corrsketches.ColumnType;
import corrsketches.benchmark.ColumnPair;
import corrsketches.benchmark.distributions.MultinomialSampler;
import corrsketches.benchmark.pairwise.ColumnCombination;
import corrsketches.benchmark.pairwise.SyntheticColumnCombination;
import corrsketches.benchmark.pairwise.TablePair;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;

public class MultinomialSyntheticSource {

  public static List<ColumnCombination> createColumnCombinations(int numberOfColumns, int seed) {
    return createColumnCombinations(numberOfColumns, new Random(seed), 10000);
  }

  public static List<ColumnCombination> createColumnCombinations(
      int numberOfColumns, int seed, int maxRows) {
    return createColumnCombinations(numberOfColumns, new Random(seed), maxRows);
  }

  public static List<ColumnCombination> createColumnCombinations(
      int numberOfColumns, Random rng, int maxRows) {
    List<ColumnCombination> combinations = new ArrayList<>();
    for (int i = 0; i < numberOfColumns; i++) {
      for (int n : Arrays.asList(16, 64, 256, 512, 1024)) {
        MultinomialParameters parameters = createMultinomialParameters(rng, n);
        for (var dataPairType : PairDataType.values()) {
          for (var keyDist : KeyDistribution.values()) {
            combinations.add(
                new MultinomialColumnCombination(
                    rng.nextInt(), parameters, new PairTypeParams(keyDist, dataPairType), maxRows));
          }
        }
      }
    }
    return combinations;
  }

  private static TablePair generateColumns(
      int maxRows, int seed, float rho, MultinomialParameters parameters, PairTypeParams params) {
    // RandomGenerator rng = new JDKRandomGenerator(seed);
    RandomGenerator rng = new Well19937c(seed);
    double[][] sampled = sampleMultinomial(maxRows, parameters, rng);
    double[] X = sampled[0];
    double[] Y = sampled[1];
    String[] K = generateJoinKeys(maxRows, params.keyDistribution, X);

    String datasetId =
        String.format(
            "sbn_r=%.5f_xtype=%s_ytype=%s_keydist=%s",
            rho, params.typeX, params.typeY, params.keyDistribution);
    String keyName = "K" + seed;
    List<String> keyValues = Arrays.asList(K);
    String columnNameX = "X" + seed;
    String columnNameY = "Y" + seed;

    ColumnPair xcp = new ColumnPair(datasetId, keyName, keyValues, columnNameX, params.typeX, X);
    ColumnPair ycp = new ColumnPair(datasetId, keyName, keyValues, columnNameY, params.typeY, Y);

    return new TablePair(xcp, ycp);
  }

  private static String[] generateJoinKeys(
      int length, KeyDistribution distribution, double[] feature) {
    final int[] keys;
    if (distribution == KeyDistribution.UNIQUE) {
      keys = sequentialKeys(length);
    } else if (distribution == KeyDistribution.SAME_AS_X) {
      keys = sameAsFeature(length, feature);
    } else {
      throw new UnsupportedOperationException();
    }
    String[] K = new String[length];
    for (int i = 0; i < length; i++) {
      K[i] = String.valueOf(keys[i]);
    }
    return K;
  }

  private static int[] sameAsFeature(int length, double[] feature) {
    final int[] key = new int[length];
    for (int i = 0; i < key.length; i++) {
      key[i] = (int) feature[i];
    }
    return key;
  }

  public static int[] sequentialKeys(int length) {
    final int[] data = new int[length];
    for (int i = 0; i < data.length; i++) {
      data[i] = i;
    }
    return data;
  }

  /**
   * Samples data points from a multinomial distribution with the given correlation.
   *
   * @param sampleSize the number of samples to generate
   * @param parameters the parameters for the Multinomial distribution
   * @param rng the random number generator
   * @return a matrix of dimensions (2, sampleSize) containing samples of the
   */
  private static double[][] sampleMultinomial(
      int sampleSize, MultinomialParameters parameters, RandomGenerator rng) {
    double[] probabilities = parameters.getProbabilities();
    var multinomialSampler =
        new MultinomialSampler(new Random(rng.nextInt()), parameters.n, probabilities);
    return multinomialSampler.sample(sampleSize);
  }

  static MultinomialParameters createMultinomialParameters(Random rng, int n) {
    double p, q;
    float r;
    do {
      final double mi = rng.nextDouble() * 3.5;
      r = (float) Math.sqrt(1. - Math.exp(-2 * mi));
      q = 0.15 + (rng.nextDouble() * 0.85); // [0.25, 0.75)
      final double b = q / (1.0 - q);
      final double a = Math.pow(r, 2) / b;
      p = a / (a + 1.0);
    } while (p < 0.15 || p > 0.85);
    return new MultinomialParameters(n, p, q);
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
      } else if (pairType == PairDataType.MIXED) {
        typeX = ColumnType.CATEGORICAL;
        typeY = ColumnType.NUMERICAL;
      } else {
        throw new UnsupportedOperationException("Invalid pair data type");
      }
    }
  }

  private enum PairDataType {
    NUMERICAL,
    DISCRETE,
    MIXED
  }

  public enum KeyDistribution {
    UNIQUE,
    SAME_AS_X,
  }

  public static class MultinomialParameters {
    public final double p;
    public final double q;
    public final int n;

    public MultinomialParameters(int n, double p, double q) {
      this.n = n;
      this.p = p;
      this.q = q;
    }

    public double[] getProbabilities() {
      return new double[] {p, q, 1. - (p + q)};
    }

    public float getCorrelation() {
      return (float) (-p * q / (Math.sqrt(p * (1 - p)) * (Math.sqrt(q * (1 - q)))));
    }

    public double getMutualInformation() {
      return calcTrinomialMI(n, p, q);
    }

    public static double calcTrinomialMI(int m, double p1, double p2) {

      final double logOfFactorialOfM = logOfFactorial(m);

      double p3 = 1.0 - (p1 + p2);
      double jointEntropy = 0.0;
      jointEntropy -= logOfFactorialOfM;
      jointEntropy -= m * (p1 * Math.log(p1) + p2 * Math.log(p2) + p3 * Math.log(p3));
      jointEntropy += sum(m, p1);
      jointEntropy += sum(m, p2);
      jointEntropy += sum(m, p3);

      double px1 = p1;
      double px2 = 1.0 - p1;
      double xEntropy = 0.0;
      xEntropy -= logOfFactorialOfM;
      xEntropy -= m * (px1 * Math.log(px1) + px2 * Math.log(px2));
      xEntropy += sum(m, px1);
      xEntropy += sum(m, px2);

      double py1 = p2;
      double py2 = 1.0 - p2;
      double yEntropy = 0.0;
      yEntropy -= logOfFactorialOfM;
      yEntropy -= m * (py1 * Math.log(py1) + py2 * Math.log(py2));
      yEntropy += sum(m, py1);
      yEntropy += sum(m, py2);

      return xEntropy + yEntropy - jointEntropy;
    }

    public static double sum(int m, double p) {
      double sum = 0.0;
      for (int i = 0; i <= m; i++) {
        final BigDecimal comb = new BigDecimal(BigIntegerMath.binomial(m, i));
        sum +=
            comb.multiply(
                    BigDecimal.valueOf(Math.pow(p, i) * Math.pow(1 - p, m - i) * logOfFactorial(i)))
                .doubleValue();
      }
      return sum;
    }

    /**
     * Calculate an approximation of the mutual information using the MI formula for the bivariate
     * normal distribution.
     *
     * @return
     */
    public double getBivariateMI() {
      double correlation = getCorrelation();
      double r2 = correlation * correlation;
      if (r2 >= 1.0) {
        r2 = Math.nextDown(1);
      }
      return -0.5 * Math.log(1.0 - r2);
    }
  }

  public static class MultinomialColumnCombination implements SyntheticColumnCombination {
    public final PairTypeParams pairTypeParams;
    public final int maxRows;
    public final float correlation;
    public final int seed;
    public MultinomialParameters multinomialParameters;

    public MultinomialColumnCombination(
        int seed,
        MultinomialParameters multinomialParameters,
        PairTypeParams pairTypeParams,
        int maxRows) {
      this.seed = seed;
      this.multinomialParameters = multinomialParameters;
      this.correlation = multinomialParameters.getCorrelation();
      this.pairTypeParams = pairTypeParams;
      this.maxRows = maxRows;
    }

    public float getCorrelation() {
      return correlation;
    }

    public float getMutualInformation() {
      return (float) multinomialParameters.getMutualInformation();
    }

    @Override
    public String getKeyDistribution() {
      return pairTypeParams.keyDistribution.toString();
    }

    public MultinomialParameters getParameters() {
      return multinomialParameters;
    }

    @Override
    public TablePair getTablePair() {
      return generateColumns(maxRows, seed, correlation, multinomialParameters, pairTypeParams);
    }
  }
}
