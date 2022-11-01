package corrsketches.kmv;

import corrsketches.aggregations.AggregateFunction;
import corrsketches.aggregations.AggregateFunction.Aggregator;
import corrsketches.aggregations.AggregateFunction.BatchAggregator;
import corrsketches.sampling.ReservoirSampler;
import corrsketches.sampling.Sampler;
import java.util.Comparator;

public class ValueHash {

  public static final Comparator<ValueHash> COMPARATOR_ASC = new UnitHashComparatorAscending();

  public final Aggregator aggregator;
  public final int keyHash;
  public final double unitHash;
  private double value;

  Sampler<Double> sampler;
  int count; // the number of items associate with this join key

//  public ValueHash(int keyHash, double unitHash, double value, AggregateFunction function) {
//    this(keyHash, unitHash, value, function, 256);
//  }

  public ValueHash(int keyHash, double unitHash, double value, AggregateFunction function, Sampler<Double> sampler) {
    this.keyHash = keyHash;
    this.unitHash = unitHash;
    this.aggregator = function.get();
    this.value = aggregator.first(value);
    this.sampler = sampler;
    this.count = 1;
    sampler.sample(value);
  }

  public void update(double value) {
    System.out.println("VH.update(): value = " + value);
    this.value = this.aggregator.update(this.value, value);
    this.sampler.sample(value);
    this.count++;
  }

  public int count() {
    return this.count;
  }

  public Sampler<Double> sampler() {
    return this.sampler;
  }

  public double value() {
    if (aggregator instanceof BatchAggregator) {
      return ((BatchAggregator) aggregator).aggregatedValue();
    }
    return value;
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
