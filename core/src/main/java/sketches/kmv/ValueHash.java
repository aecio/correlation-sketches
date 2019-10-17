package sketches.kmv;

import java.util.Comparator;

public class ValueHash {

  public static final Comparator<ValueHash> COMPARATOR_ASC = new HashValueComparatorAscending();

  public int hashValue;
  public double grmHash;
  public double value;

  public ValueHash(int hashValue, double grmHash, double value) {
    this.hashValue = hashValue;
    this.grmHash = grmHash;
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    return hashValue == ((ValueHash) o).hashValue;
  }

  @Override
  public int hashCode() {
    return hashValue;
  }

  @Override
  public String toString() {
    return "ValueHash{hashValue=" + hashValue + ", grmHash=" + grmHash + ", value=" + value + '}';
  }

  private static class HashValueComparatorAscending implements Comparator<ValueHash> {
    @Override
    public int compare(ValueHash a, ValueHash b) {
      return Double.compare(a.grmHash, b.grmHash);
    }
  }
}
