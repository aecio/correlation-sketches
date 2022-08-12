package corrsketches.correlation;

import corrsketches.correlation.MutualInformation.MI;
import corrsketches.statistics.DifferentialEntropy;
import corrsketches.statistics.Entropy;

public class MutualInformationMixed implements Correlation<MI> {

  public static final MutualInformationMixed INSTANCE = new MutualInformationMixed();

  private final int k;

  public MutualInformationMixed() {
    this(3);
  }

  public MutualInformationMixed(int k) {
    this.k = k;
  }

  @Override
  public MI ofCategorical(int[] x, int[] y) {
    return MutualInformation.ofCategorical(x, y);
  }

  @Override
  public MI ofCategoricalNumerical(int[] x, double[] y) {
    final double ex = Entropy.entropy(x);
    final double ey = DifferentialEntropy.entropy(y, k);
    final double mi = MutualInformationDC.mi(x, y, k);
    return new MI(mi, x.length, ex, ey);
  }

  @Override
  public MI ofNumericalCategorical(double[] x, int[] y) {
    final double ex = DifferentialEntropy.entropy(x);
    final double ey = Entropy.entropy(y);
    final double mi = MutualInformationDC.mi(y, x);
    return new MI(mi, x.length, ex, ey);
  }
}
