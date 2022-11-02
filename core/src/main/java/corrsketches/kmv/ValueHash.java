package corrsketches.kmv;

import corrsketches.aggregations.AggregateFunction;
import corrsketches.aggregations.AggregateFunction.Aggregator;
import corrsketches.sampling.DoubleSampler;
import java.util.Comparator;

public class ValueHash {

  public static final Comparator<ValueHash> COMPARATOR_ASC = new UnitHashComparatorAscending();

  public final Aggregator aggregator;
  public final int keyHash;
  public final double unitHash;

  DoubleSampler sampler;
  int count; // the number of items associate with this join key

  public ValueHash(
      int keyHash,
      double unitHash,
      double value,
      AggregateFunction function,
      DoubleSampler sampler) {
    this.keyHash = keyHash;
    this.unitHash = unitHash;
    this.aggregator = function.get();
    if (aggregator != null) {
      this.aggregator.first(value);
    }
    this.sampler = sampler;
    this.count = 1;
    sampler.sample(value);
  }

  public void update(double value) {
    if (this.aggregator != null) {
      this.aggregator.update(value);
    }
    if (this.sampler != null) {
      this.sampler.sample(value);
    }
    this.count++;
  }

  public int count() {
    return this.count;
  }

  public DoubleSampler sampler() {
    return this.sampler;
  }

  public double value() {
    return aggregator.aggregatedValue();
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
    return "ValueHash{keyHash="
        + keyHash
        + ", unitHash="
        + unitHash
        + ", aggregator="
        + aggregator
        + ", sampler: "
        + sampler
        + '}';
  }

  private static class UnitHashComparatorAscending implements Comparator<ValueHash> {
    @Override
    public int compare(ValueHash a, ValueHash b) {
      return Double.compare(a.unitHash, b.unitHash);
    }
  }
}
