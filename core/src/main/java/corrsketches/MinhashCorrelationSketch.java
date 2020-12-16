package corrsketches;

import corrsketches.correlation.PearsonCorrelation;
import corrsketches.util.Hashes;
import corrsketches.util.MinwiseHasher;
import corrsketches.util.MinwiseHasher.Signatures;
import it.unimi.dsi.fastutil.ints.Int2DoubleAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import java.util.List;

public class MinhashCorrelationSketch {

  private static final MinwiseHasher minHasher = new MinwiseHasher(64);
  private static final Hashes hashFunction = new Hashes();

  private final int[] minhashes;
  private final double[] values;

  public MinhashCorrelationSketch(List<String> keys, double[] values, int numberOfHashes) {
    this(new MinwiseHasher(numberOfHashes).signature(hashFunction.murmur3_32(keys)), values);
  }

  public MinhashCorrelationSketch(List<String> keys, double[] values) {
    this(minHasher.signature(hashFunction.murmur3_32(keys)), values);
  }

  public MinhashCorrelationSketch(List<String> keys, double[] values, MinwiseHasher minHasher) {
    this(minHasher.signature(hashFunction.murmur3_32(keys)), values);
  }

  public MinhashCorrelationSketch(Signatures signatures, double[] values) {
    this.minhashes = new int[signatures.size];
    this.values = new double[signatures.size];
    for (int i = 0; i < signatures.size; i++) {
      if (signatures.positions[i] > values.length) {
        throw new IllegalArgumentException(
            String.format(
                "%s: position=%d values.length=%d",
                "signature position references a value outside bounds",
                signatures.positions[i],
                values.length));
      }
      this.minhashes[i] = signatures.hashes[i];
      this.values[i] = values[signatures.positions[i]];
    }
  }

  public double correlationTo(MinhashCorrelationSketch other) {
    // compute intersection between both sketches
    IntAVLTreeSet commonHashes = new IntAVLTreeSet(this.minhashes);
    commonHashes.retainAll(new IntAVLTreeSet(other.minhashes));
    if (commonHashes.size() == 0) {
      //            return Double.NaN;
      throw new IllegalArgumentException("No overlap between sets");
    }
    // build values vectors for common murmur3_32
    Int2DoubleMap thisMap = new Int2DoubleAVLTreeMap();
    Int2DoubleMap otherMap = new Int2DoubleAVLTreeMap();
    for (int i = 0; i < this.minhashes.length; i++) {
      thisMap.putIfAbsent(this.minhashes[i], this.values[i]);
      otherMap.putIfAbsent(other.minhashes[i], other.values[i]);
    }
    double[] thisValues = new double[commonHashes.size()];
    double[] otherValues = new double[commonHashes.size()];
    int i = 0;
    for (int hash : commonHashes) {
      if (!thisMap.containsKey(hash)) {
        throw new IllegalStateException("common hash not found in 'this' map.");
      }
      thisValues[i] = thisMap.get(hash);
      otherValues[i] = otherMap.get(hash);
      //            System.out.printf("thisValues[%d]=%.6f otherValues[%d]=%.6f\n", i,
      // thisValues[i], i, otherValues[i]);
      i++;
    }
    // finally, compute correlation coefficient between common values
    return PearsonCorrelation.coefficient(thisValues, otherValues);
  }
}
