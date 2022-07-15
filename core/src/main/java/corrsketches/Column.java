package corrsketches;

import java.util.Arrays;

public class Column {

  public final double[] values;
  public final ColumnType type;

  public Column(double[] values, ColumnType type) {
    this.values = values;
    this.type = type;
  }

  public static Column of(double[] values, ColumnType type) {
    return new Column(values, type);
  }

  public static Column numerical(double... values) {
    return of(values, ColumnType.NUMERICAL);
  }

  public static Column categorical(double... values) {
    return of(values, ColumnType.CATEGORICAL);
  }

  @Override
  public String toString() {
    return "Column{" + "values=" + Arrays.toString(values) + ", type=" + type + '}';
  }

  public int[] valuesAsIntArray() {
    return castToIntArray(values);
  }

  static int[] castToIntArray(double[] x) {
    int[] xi = new int[x.length];
    for (int i = 0; i < x.length; i++) {
      xi[i] = (int) x[i];
    }
    return xi;
  }
}
