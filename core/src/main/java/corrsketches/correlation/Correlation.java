package corrsketches.correlation;

import static com.google.common.base.Preconditions.checkArgument;

import corrsketches.Column;
import corrsketches.ColumnType;

public interface Correlation {

  default Estimate of(Column x, Column y) {
    checkArgument(x.values.length == y.values.length, "x and y must have same size");
    if (x.type == y.type) {
      // x and y have the same type
      if (x.type == ColumnType.CATEGORICAL) {
        // both are categorical
        return ofCategorical(x.valuesAsIntArray(), y.valuesAsIntArray());
      }
      if (x.type == ColumnType.NUMERICAL) {
        // both are numerical
        return ofNumerical(x.values, y.values);
      }
    } else {
      // x and y have different types
      if (x.type == ColumnType.CATEGORICAL) { // and y is NUMERICAL
        return ofCategoricalNumerical(x.valuesAsIntArray(), y.values);
      }
      if (x.type == ColumnType.NUMERICAL) { // and y is CATEGORICAL
        return ofNumericalCategorical(x.values, y.valuesAsIntArray());
      }
    }
    throw new IllegalStateException("Variable types must be either CATEGORICAL or NUMERICAL");
  }

  default Estimate ofNumerical(double[] x, double[] y) {
    throw new UnsupportedOperationException(
        getClass() + " does not support correlation for numerical variables");
  }

  default Estimate ofCategorical(int[] x, int[] y) {
    throw new UnsupportedOperationException(
        getClass() + " does not support correlation for categorical variables");
  }

  default Estimate ofNumericalCategorical(final double[] y, final int[] x) {
    throw new UnsupportedOperationException(
        getClass() + " does not support correlation for numerical-categorical variables");
  }

  default Estimate ofCategoricalNumerical(final int[] x, final double[] y) {
    throw new UnsupportedOperationException(
        getClass() + " does not support correlation for categorical-numerical variables");
  }
}
