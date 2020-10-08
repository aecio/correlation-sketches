package corrsketches.kmv;

import java.util.Comparator;

public class ValueHash {

  public static final Comparator<ValueHash> COMPARATOR_ASC = new UnitHashComparatorAscending();

  public int keyHash;
  public double unitHash;
  public double value;

  public ValueHash(int keyHash, double unitHash, double value) {
    this.keyHash = keyHash;
    this.unitHash = unitHash;
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    return keyHash == ((ValueHash) o).keyHash;
  }

  @Override
  public int hashCode() {
    return keyHash;
  }

  @Override
  public String toString() {
    return "ValueHash{keyHash=" + keyHash + ", unitHash=" + unitHash + ", value=" + value + '}';
  }

  private static class UnitHashComparatorAscending implements Comparator<ValueHash> {
    @Override
    public int compare(ValueHash a, ValueHash b) {
      return Double.compare(a.unitHash, b.unitHash);
    }
  }
}
