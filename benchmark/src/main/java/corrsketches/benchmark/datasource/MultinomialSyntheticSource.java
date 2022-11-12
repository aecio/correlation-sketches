package corrsketches.benchmark.datasource;

import corrsketches.ColumnType;
import corrsketches.benchmark.ColumnPair;
import corrsketches.benchmark.distributions.MultinomialSampler;
import corrsketches.benchmark.pairwise.ColumnCombination;
import corrsketches.benchmark.pairwise.SyntheticColumnCombination;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;

public class MultinomialSyntheticSource {

  public static List<ColumnCombination> createColumnCombinations(int numberOfColumns) {
    return createColumnCombinations(numberOfColumns, new Random(1234));
  }

  public static List<ColumnCombination> createColumnCombinations(int numberOfColumns, Random rng) {
    List<ColumnCombination> combinations = new ArrayList<>();
    for (int i = 0; i < numberOfColumns; i++) {
      MultinomialParameters parameters = createMultinomialParameters(rng);
      for (var dataPairType : PairDataType.values()) {
        for (var keyDist : KeyDistribution.values()) {
          combinations.add(
              new MultinomialColumnCombination(
                  rng.nextInt(), parameters, new PairTypeParams(keyDist, dataPairType)));
        }
      }
    }
    return combinations;
  }

  private static Pair generateColumns(
      int maxRows, int seed, float rho, MultinomialParameters parameters, PairTypeParams params) {
    // RandomGenerator rng = new JDKRandomGenerator(seed);
    RandomGenerator rng = new Well19937c(seed);
    double[][] sampled = sampleMultinomial(maxRows, parameters, rng);
    double[] X = sampled[0];
    double[] Y = sampled[1];
    String[] K = generateJoinKeys(maxRows, params.keyDistribution, Y, rng);

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

    return new Pair(xcp, ycp);
  }

  private static String[] generateJoinKeys(
      int length, KeyDistribution distribution, double[] feature, RandomGenerator rng) {
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

  private static MultinomialParameters createMultinomialParameters(Random rng) {
    int n = 512;
    double p, q;
    float r;
    do {
      r = Math.round(rng.nextDouble() * 1000.0) / 1000f;
      q = 0.15 + (rng.nextDouble() * 0.85); // [0.25, 0.75)
      final double b = q / (1.0 - q);
      final double a = Math.pow(r, 2) / b;
      p = a / (a + 1.0);
    } while (p < 0.15 || p > 0.85);
    //    System.out.printf("p = %.5f  q = %.5f  n = %d  r = %.5f\n", p, q, n, r);
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
    UNIQUE,
    SAME_AS_X,
  }

  private static class Pair {

    public final ColumnPair y;
    public final ColumnPair x;

    public Pair(ColumnPair x, ColumnPair y) {
      this.x = x;
      this.y = y;
    }
  }

  static class MultinomialParameters {
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
  }

  public static class MultinomialColumnCombination implements SyntheticColumnCombination {

    private final int MAX_ROWS = 10000;
    public final PairTypeParams pairTypeParams;
    public final float correlation;
    public final int seed;
    public MultinomialParameters multinomialParameters;
    private Pair pair;

    public MultinomialColumnCombination(
        int seed, MultinomialParameters multinomialParameters, PairTypeParams pairTypeParams) {
      this.seed = seed;
      this.multinomialParameters = multinomialParameters;
      this.correlation = multinomialParameters.getCorrelation();
      this.pairTypeParams = pairTypeParams;
    }

    public ColumnPair getX() {
      if (this.pair == null) {
        this.pair =
            generateColumns(MAX_ROWS, seed, correlation, multinomialParameters, pairTypeParams);
      }
      return pair.x;
    }

    public ColumnPair getY() {
      if (this.pair == null) {
        this.pair =
            generateColumns(MAX_ROWS, seed, correlation, multinomialParameters, pairTypeParams);
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
