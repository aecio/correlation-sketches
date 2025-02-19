package corrsketches;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.Arrays;

public class Table {

  public final int[] keys; // join keys sorted in ascending order
  public final Column values; // values associated with the join keys
  public final boolean uniqueKeys; // whether the values in the join key are unique

  public Table(int[] keys, Column values, boolean uniqueKeys) {
    this.keys = keys;
    this.values = values;
    this.uniqueKeys = uniqueKeys;
  }

  public static Join join(Table left, Table right) {
    if (left.uniqueKeys && right.uniqueKeys) {
      return Table.joinOneToOne(left, right);
    }
    return Table.innerJoin(left, right);
  }

  /**
   * Computes inner join between the two tables assuming that the keys of both sketches are
   * pre-sorted in increasing order.
   */
  public static Join innerJoin(Table left, Table right) {
    final int initialCapacity = Math.max(left.keys.length, right.keys.length);
    IntArrayList k = new IntArrayList(initialCapacity);
    DoubleArrayList l = new DoubleArrayList(initialCapacity);
    DoubleArrayList r = new DoubleArrayList(initialCapacity);
    int lidx = 0;
    int ridx = 0;
    int i, j;
    while (lidx < left.keys.length && ridx < right.keys.length) {
      if (left.keys[lidx] < right.keys[ridx]) {
        lidx++;
      } else if (left.keys[lidx] > right.keys[ridx]) {
        ridx++;
      } else {
        // keys are equal, iterate over all pairs of indexes containing this key
        i = lidx;
        j = ridx;
        while (i < left.keys.length && left.keys[i] == left.keys[lidx]) {
          j = ridx;
          while (j < right.keys.length && right.keys[j] == right.keys[ridx]) {
            k.add(left.keys[i]);
            l.add(left.values.values[i]);
            r.add(right.values.values[j]);
            j++;
          }
          i++;
        }
        lidx = i;
        ridx = j;
      }
    }
    return new Join(
        k.toIntArray(),
        Column.of(l.toDoubleArray(), left.values.type),
        Column.of(r.toDoubleArray(), right.values.type));
  }

  /**
   * Joins the tables assuming that the keys of both sketches are unique (primary-keys) and
   * pre-sorted in increasing order.
   */
  public static Join joinOneToOne(Table left, Table right) {
    final int capacity = Math.max(left.keys.length, right.keys.length);
    IntArrayList k = new IntArrayList(capacity);
    DoubleArrayList l = new DoubleArrayList(capacity);
    DoubleArrayList r = new DoubleArrayList(capacity);
    int lidx = 0;
    int ridx = 0;
    while (lidx < left.keys.length && ridx < right.keys.length) {
      if (left.keys[lidx] < right.keys[ridx]) {
        lidx++;
      } else if (left.keys[lidx] > right.keys[ridx]) {
        ridx++;
      } else {
        // keys are equal
        k.add(left.keys[lidx]);
        l.add(left.values.values[lidx]);
        r.add(right.values.values[ridx]);
        lidx++;
        ridx++;
      }
    }
    return new Join(
        k.toIntArray(),
        Column.of(l.toDoubleArray(), left.values.type),
        Column.of(r.toDoubleArray(), right.values.type));
  }

  public static class Join {

    public final int[] keys;
    public final Column left;
    public final Column right;

    public Join(int[] keys, Column left, Column right) {
      this.keys = keys;
      this.left = left;
      this.right = right;
    }

    @Override
    public String toString() {
      return "Join{\n"
          + "  keys="
          + Arrays.toString(keys)
          + ",\n  left="
          + left
          + ",\n  right="
          + right
          + "\n}";
    }
  }
}
