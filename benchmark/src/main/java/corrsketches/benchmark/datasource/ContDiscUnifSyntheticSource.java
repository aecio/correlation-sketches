package corrsketches.benchmark.datasource;

import corrsketches.ColumnType;
import corrsketches.benchmark.ColumnPair;
import corrsketches.benchmark.pairwise.ColumnCombination;
import corrsketches.benchmark.pairwise.SyntheticColumnCombination;
import corrsketches.benchmark.pairwise.TablePair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class ContDiscUnifSyntheticSource {

  public static List<ColumnCombination> createColumnCombinations(int numberOfColumns, int seed) {
    return createColumnCombinations(numberOfColumns, new Random(seed));
  }

  public static List<ColumnCombination> createColumnCombinations(int numberOfColumns, Random rng) {
    List<ColumnCombination> combinations = new ArrayList<>();
    for (int i = 0; i < numberOfColumns; i++) {
      for (int m : Arrays.asList(5, 50, 500)) {
        Parameters parameters = new Parameters(m);
        for (var dataPairType : PairDataType.values()) {
          for (var keyDist : KeyDistribution.values()) {
            combinations.add(
                new ContUnifDiscUnifColumnCombination(
                    rng.nextInt(), parameters, new PairTypeParams(keyDist, dataPairType)));
          }
        }
      }
    }
    return combinations;
  }

  private static TablePair generateColumns(
      int maxRows, int seed, Parameters parameters, PairTypeParams params) {
    //    RandomGenerator rng = new JDKRandomGenerator(seed);
    //    Random rng = new Well19937c(seed);
    Random rng = new Random(seed);
    double[][] sampled = sampleDistribution(maxRows, parameters, rng);
    double[] X = sampled[0];
    double[] Y = sampled[1];
    String[] K = generateJoinKeys(maxRows, params.keyDistribution, X);

    String datasetId =
        String.format(
            "cdunif_m=%d_xtype=%s_ytype=%s_keydist=%s",
            parameters.m, params.typeX, params.typeY, params.keyDistribution);
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
   * Samples two random variables X and Y from the following distribution: X is a discrete random
   * variable and Y is a continuous random variable. X is uniformly distributed over integers {0, 1,
   * ..., m − 1} and Y is uniformly distributed over the range [X, X + 2] for a given X.
   *
   * @param sampleSize the number of samples to generate
   * @param parameters the parameters m for the distribution
   * @param rng the random number generator
   * @return a matrix of dimensions (2, sampleSize) containing samples of the
   */
  private static double[][] sampleDistribution(int sampleSize, Parameters parameters, Random rng) {
    final int m = parameters.m;
    final double[][] data = new double[2][sampleSize];
    for (int i = 0; i < sampleSize; i++) {
      final double x = rng.nextInt(m);
      final double y = x + rng.nextDouble() * 2;
      data[0][i] = x;
      data[1][i] = y;
    }
    return data;
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

  public static class Parameters {
    public final int m;

    public Parameters(int m) {
      this.m = m;
    }
  }

  public static class ContUnifDiscUnifColumnCombination implements SyntheticColumnCombination {

    private final int MAX_ROWS = 10000;
    public final PairTypeParams pairTypeParams;
    public final int seed;
    public Parameters parameters;

    public ContUnifDiscUnifColumnCombination(
        int seed, Parameters parameters, PairTypeParams pairTypeParams) {
      this.seed = seed;
      this.parameters = parameters;
      this.pairTypeParams = pairTypeParams;
    }

    public float getMutualInformation() {
      final double m = parameters.m;
      return (float) (Math.log(m) - (m - 1) * Math.log(2) / m);
    }

    @Override
    public String getKeyDistribution() {
      return pairTypeParams.keyDistribution.toString();
    }

    public Parameters getParameters() {
      return parameters;
    }

    @Override
    public TablePair getTablePair() {
      return generateColumns(MAX_ROWS, seed, parameters, pairTypeParams);
    }
  }
}
