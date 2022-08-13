package corrsketches.correlation;

import corrsketches.correlation.MutualInformation.MI;
import corrsketches.statistics.DifferentialEntropyMixed;
import corrsketches.statistics.Entropy;

public class MutualInformationDiffEntMixed implements Correlation<MI> {

  public static final MutualInformationDiffEntMixed INSTANCE = new MutualInformationDiffEntMixed();

  private final int k;

  public MutualInformationDiffEntMixed() {
    this(3);
  }

  public MutualInformationDiffEntMixed(int k) {
    this.k = k;
  }

  @Override
  public MI ofCategorical(int[] x, int[] y) {
    return MutualInformation.ofCategorical(x, y);
  }

  @Override
  public MI ofCategoricalNumerical(int[] x, double[] y) {
    final double ex = Entropy.entropy(x);
    final double ey = DifferentialEntropyMixed.entropy(y, k);
    final double mi = MutualInformationDC.mi(x, y, k);
    return new MI(mi, x.length, ex, ey);
  }

  @Override
  public MI ofNumericalCategorical(double[] x, int[] y) {
    final double ex = DifferentialEntropyMixed.entropy(x, k);
    final double ey = Entropy.entropy(y);
    final double mi = MutualInformationDC.mi(y, x, k);
    return new MI(mi, x.length, ex, ey);
  }

  @Override
  public MI ofNumerical(double[] x, double[] y) {
    final double ex = DifferentialEntropyMixed.entropy(x, k);
    final double ey = DifferentialEntropyMixed.entropy(y, k);
    final double mi = MutualInformationMixedKSG.mi(y, x, k);
    return new MI(mi, x.length, ex, ey);
  }
}
