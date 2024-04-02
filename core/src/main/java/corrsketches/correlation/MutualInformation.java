package corrsketches.correlation;

import static com.google.common.base.Preconditions.checkArgument;
import static corrsketches.Column.castToIntArray;

import corrsketches.Column;

public class MutualInformation {

  public static Estimate estimateMi(final double[] x, final double[] y) {
    return new Estimate(MutualInformation.ofCategorical(x, y).value, x.length);
  }

  public static Estimate estimateNmiSqrt(double[] x, double[] y) {
    return new Estimate(MutualInformation.ofCategorical(x, y).nmiSqrt(), x.length);
  }

  public static Estimate estimateNmiMax(double[] x, double[] y) {
    return new Estimate(MutualInformation.ofCategorical(x, y).nmiMax(), x.length);
  }

  public static Estimate estimateNmiMin(double[] x, double[] y) {
    return new Estimate(MutualInformation.ofCategorical(x, y).nmiMin(), x.length);
  }

  public static MIEstimate ofCategorical(final Column x, final Column y) {
    return ofCategorical(x.values, y.values);
  }

  /**
   * Computes the (Normalized) Mutual Information (MI) between the elements of {@param x} and
   * {@param y}. This functions assumes that the values in {@param x} and {@param y} are integers
   * that identify categorical data entries and explicitly casts their double values to ints.
   */
  public static MIEstimate ofCategorical(final double[] x, final double[] y) {
    checkArgument(x.length == y.length, "x and y must have same size");
    int[] xi = castToIntArray(x);
    int[] yi = castToIntArray(y);
    return ofCategorical(xi, yi);
  }

  public static MIEstimate ofCategorical(int[] x, int[] y) {
    checkArgument(x.length == y.length, "x and y must have same size");
    return MutualInformationMLE.mi(x, y);
//    return new MI(mi.value, mi.sampleSize, entropyFromProbs(mi.px), entropyFromProbs(mi.py), mi.nx(), mi.ny());
  }
}
