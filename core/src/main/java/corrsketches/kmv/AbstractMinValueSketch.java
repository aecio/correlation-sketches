package corrsketches.kmv;

import corrsketches.aggregations.AggregateFunction;
import corrsketches.aggregations.RepeatedValueHandlerProvider;
import corrsketches.sampling.SamplerProvider;
import corrsketches.util.Hashes;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

public abstract class AbstractMinValueSketch<T> {

  protected final TreeSet<ValueHash> kMinValues;
  protected final Int2ObjectOpenHashMap<ValueHash> valueHashMap;
  protected final RepeatedValueHandlerProvider aggregatorProvider;
  protected double kthValue = Double.MIN_VALUE;
  protected int kMinItems;
  protected int seenItems;

  public AbstractMinValueSketch(Builder<?> builder) {
    this.aggregatorProvider = builder.aggregateFunction;
    this.kMinValues = new TreeSet<>(ValueHash.COMPARATOR_ASC);
    if (builder.expectedSize() < 1) {
      this.valueHashMap = new Int2ObjectOpenHashMap<>();
    } else {
      this.valueHashMap = new Int2ObjectOpenHashMap<>(builder.expectedSize() + 1);
    }
  }

  protected ValueHash createOrUpdateValueHash(int hash, double value, double hu) {
    ValueHash vh = valueHashMap.get(hash);
    if (vh == null) {
      vh = new ValueHash(hash, hu, value, aggregatorProvider.create());
      valueHashMap.put(hash, vh);
    } else {
      vh.update(value);
    }
    return vh;
  }

  /**
   * Updates this synopsis with the hashes of all the given key strings and their associated values
   */
  public void updateAll(List<String> keys, double[] values) {
    if (keys.size() != values.length) {
      throw new IllegalArgumentException("keys and values must have equal size.");
    }
    for (int i = 0; i < values.length; i++) {
      this.update(keys.get(i), values[i]);
    }
  }

  /** Updates this synopsis with the given pre-computed key hashes and their associated values */
  public void updateAll(int[] hashedKeys, double[] values) {
    if (hashedKeys.length != values.length) {
      throw new IllegalArgumentException("hashedKeys and values must have equal size.");
    }
    for (int i = 0; i < hashedKeys.length; i++) {
      update(hashedKeys[i], values[i]);
    }
  }

  /**
   * Updates this synopsis with the hash value (Murmur3) of the given key string and its associated
   * value.
   */
  void update(String key, double value) {
    if (key == null || key.isEmpty()) {
      return;
    }
    int keyHash = Hashes.murmur3_32(key);
    this.update(keyHash, value);
  }

  /** Updates this synopsis with the given hashed key */
  public abstract void update(int hash, double value);

  /** The improved (unbiased) distinct value estimator (UB) from Beyer et. al., SIGMOD 2007. */
  public double distinctValues() {
    return (kMinValues.size() - 1.0) / kthValue;
  }

  /** Basic distinct value estimator (BE) from Beyer et. al., SIGMOD 2007. */
  public double distinctValuesBE() {
    return kMinValues.size() / kthValue;
  }

  /** Estimates the size of union of the given KMV synopsis */
  public abstract double unionSize(T other);

  /** Estimates intersection between the sets represented by this synopsis and the other. */
  public abstract double intersectionSize(T other);

  /** Estimates the Jaccard similarity between this and the other synopsis */
  public abstract double jaccard(T other);

  /**
   * Estimates the jaccard containment (JC) of the set represented by this synopsis with the other
   * synopsis.
   *
   * <p>JC(X, Y) = |X ∩ Y| / |X| = |this ∩ other| / |this|
   */
  public double containment(T other) {
    return this.intersectionSize(other) / this.distinctValues();
  }

  public TreeSet<ValueHash> getKMinValues() {
    return this.kMinValues;
  }

  // TODO: Use implementation from Sets.intersectionSize
  protected static int intersectionSize(TreeSet<ValueHash> x, TreeSet<ValueHash> y) {
    HashSet<ValueHash> intersection = new HashSet<>(x);
    intersection.retainAll(y);
    return intersection.size();
  }

  public RepeatedValueHandlerProvider aggregatorProvider() {
    return aggregatorProvider;
  }

  public boolean isAggregate() {
    return aggregatorProvider.create().isAggregator();
  }

  public abstract Samples getSamples();

  public class Samples {
    public int[] keys;
    public double[] values;
    public boolean uniqueKeys;

    public Samples(int[] keys, double[] values, boolean uniqueKeys) {
      this.keys = keys;
      this.values = values;
      this.uniqueKeys = uniqueKeys;
    }
  }

  public abstract static class Builder<T extends AbstractMinValueSketch<T>> {

    protected RepeatedValueHandlerProvider aggregateFunction =
        AggregateFunction.FIRST.getProvider();

    protected int expectedSize() {
      return -1;
    }

    public Builder<T> aggregate(AggregateFunction aggregateFunction) {
      this.aggregateFunction = aggregateFunction.getProvider();
      return this;
    }

    public Builder<T> aggregate(SamplerProvider sampler) {
      this.aggregateFunction = sampler;
      return this;
    }

    /** Creates an empty min-values sketch. */
    public abstract T build();

    /**
     * Creates a min-values sketch using the given key values and their associated numeric values.
     */
    public T buildFromKeys(List<String> keys, double[] values) {
      final T sketch = build();
      sketch.updateAll(keys, values);
      return sketch;
    }

    /**
     * Creates a min-values sketch from the given array of pre-computed hashed keys and their
     * associated values.
     */
    public T buildFromHashedKeys(int[] hashedKeys, double[] values) {
      final T sketch = build();
      sketch.updateAll(hashedKeys, values);
      return sketch;
    }
  }
}
