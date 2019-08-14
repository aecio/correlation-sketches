package sketches.correlation;

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleRBTreeMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import java.util.List;
import sketches.correlation.MinwiseHasher.Signatures;

public class CorrelationSketch {

    private static MinwiseHasher minHasher = new MinwiseHasher(64);
    private static MurmurHasher hashFunction = new MurmurHasher();

    private int[] minhashes;
    private double[] values;

    public CorrelationSketch(List<String> keys, double[] values, int numberOfHashes) {
        this(new MinwiseHasher(numberOfHashes).signature(hashFunction.hashes(keys)), values);
    }

    public CorrelationSketch(List<String> keys, double[] values) {
        this(minHasher.signature(hashFunction.hashes(keys)), values);
    }

    public CorrelationSketch(List<String> keys, double[] values, MinwiseHasher minHasher) {
        this(minHasher.signature(hashFunction.hashes(keys)), values);
    }

    public CorrelationSketch(Signatures signatures, double[] values) {
        this.minhashes = new int[signatures.size];
        this.values = new double[signatures.size];
        for (int i = 0; i < signatures.size; i++) {
            this.minhashes[i] = signatures.hashes[i];
            this.values[i] = values[signatures.positions[i]];
        }
    }

    public double correlationTo(CorrelationSketch other) {
        // compute intersection between both sketches
        IntAVLTreeSet commonHashes = new IntAVLTreeSet(this.minhashes);
        commonHashes.retainAll(new IntAVLTreeSet(other.minhashes));
        // build values vectors for common hashes
        Int2DoubleMap thisMap = new Int2DoubleRBTreeMap();
        Int2DoubleMap otherMap = new Int2DoubleRBTreeMap();
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
            i++;
        }
        // finally, compute correlation coefficient between common values
        return PearsonCorrelation.coefficient(thisValues, otherValues);
    }

}
