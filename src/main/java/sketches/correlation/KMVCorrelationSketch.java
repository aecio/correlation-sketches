package sketches.correlation;

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

public class KMVCorrelationSketch {

    private static final int DEFAULT_K = 256;

    private int k;
    private TreeSet<ValueHash> kMinValues;
    private int cardinality = -1;
    private double largestGrmhash = Double.MIN_VALUE;

    public KMVCorrelationSketch(List<String> keys, double[] values) {
        this(keys, values, DEFAULT_K);
    }

    public KMVCorrelationSketch(List<String> keys, double[] values, int k) {
        this.k = k;
        this.kMinValues = new TreeSet<>(ValueHash.COMPARATOR_ASC);
        this.updateAll(keys, values);
    }

    private KMVCorrelationSketch() {
    }

    public static KMVCorrelationSketch fromStringHashes(String[] hashes, double[] values) {
        if (hashes.length != values.length) {
            throw new IllegalArgumentException(
                    "Number of values cannot be different from number of hashes");
        }
        KMVCorrelationSketch kmv = new KMVCorrelationSketch();
        kmv.k = hashes.length;
        kmv.kMinValues = new TreeSet<>(ValueHash.COMPARATOR_ASC);
        for (int i = 0; i < hashes.length; i++) {
            kmv.internalUpdate(Integer.parseInt(hashes[i]), values[i]);
        }
        return kmv;
    }

    public void updateAll(List<String> keys, double[] values) {
//        System.err.println("updateAll()");
        if (keys.size() != values.length) {
            throw new IllegalArgumentException("keys and values must have equal size.");
        }
        for (int i = 0; i < values.length; i++) {
            update(keys.get(i), values[i]);
        }
    }

    public void update(String key, double value) {
//        System.err.println("update()");
        if (key == null || key.isEmpty()) {
            return;
        }
        int hashvalue = Hashes.murmur3_32(key);
        internalUpdate(hashvalue, value);
//        System.err.printf("kmin.size: %d largest: %.4f\n", kMinValues.size(), largestGrmhash);
    }

    private void internalUpdate(int hashvalue, double value) {
        double grmHash = Hashes.grm(hashvalue);
        if (kMinValues.size() < k) {
//            System.err.printf("kmin < k. Adding %.4f\n", grmHash);
            kMinValues.add(new ValueHash(hashvalue, grmHash, value));
            if (grmHash > largestGrmhash) {
                largestGrmhash = grmHash;
            }
        } else if (grmHash < largestGrmhash) {
//            System.err.println("kmin >=k. Removing k-min value...");
            kMinValues.remove(kMinValues.last());
            kMinValues.add(new ValueHash(hashvalue, grmHash, value));
            largestGrmhash = kMinValues.last().grmHash;
        }
        if (kMinValues.size() > k) {
            throw new IllegalStateException("This should never happen.");
        }
    }

    public void setCardinality(int cardinality) {
        this.cardinality = cardinality;
    }

    public double cardinality() {
        if (this.cardinality != -1) {
            return this.cardinality;
        }
        int k = this.kMinValues.size();
        TreeSet<ValueHash> kMinValuesDesc = new TreeSet<>(ValueHash.COMPARATOR_ASC);
        kMinValuesDesc.addAll(this.kMinValues);
        double kthValue = kMinValuesDesc.last().grmHash;
        return (k - 1.) / kthValue;
    }

    public double unionSize(KMVCorrelationSketch other) {
        int k = Math.min(this.kMinValues.size(), other.kMinValues.size());
        double kthValue = kthValueUnion(other);
        return (k - 1) / kthValue;
    }

    private double kthValueUnion(KMVCorrelationSketch other) {
        TreeSet<ValueHash> union = new TreeSet<>(ValueHash.COMPARATOR_ASC);
        union.addAll(this.kMinValues);
        union.addAll(other.kMinValues);
        return union.last().grmHash;
    }

    public double intersectionSize(KMVCorrelationSketch other) {
        int k = Math.min(this.kMinValues.size(), other.kMinValues.size());
        double kthValue = kthValueUnion(other);
        double intersectionSize = hashesIntersectionSize(this, other);
//        System.out.println("intersect: " + kthValueUnion);
        return (intersectionSize / k) * ((k - 1) / kthValue);
    }

    private static int hashesIntersectionSize(KMVCorrelationSketch a, KMVCorrelationSketch b) {
        HashSet<ValueHash> intersection = new HashSet<>(a.kMinValues);
        intersection.retainAll(b.kMinValues);
        return intersection.size();
    }

    public double jaccard(KMVCorrelationSketch other) {
        double js = this.intersectionSize(other) / this.unionSize(other);
        return js;
    }

    public double containment(KMVCorrelationSketch other) {
        double cardX = this.cardinality();
        double cardY = other.cardinality();
        double intersectionSize = this.intersectionSize(other);

        double maxJcx = Math.min(cardX, cardY) / cardX;
        double jcx = intersectionSize / cardX;
        if (jcx > maxJcx) {
            System.err.printf(
                    "jcx=%.6f max-jcx=%.6f error=%.6f card-x=%+.4f card-y=%+.4f intersection=%+.4f\n",
                    jcx, maxJcx, jcx - maxJcx, cardX, cardY, intersectionSize);
        }
        return Math.min(jcx, maxJcx);
    }

    public double correlationTo(KMVCorrelationSketch other) {

        int[] thisMinhashes = new int[this.kMinValues.size()];
        Iterator<ValueHash> thisIt = this.kMinValues.iterator();
        for (int i = 0; i < thisMinhashes.length; i++) {
            thisMinhashes[i] = thisIt.next().hashValue;
        }

        int[] otherMinhashes = new int[other.kMinValues.size()];
        Iterator<ValueHash> otherIt = other.kMinValues.iterator();
        for (int i = 0; i < otherMinhashes.length; i++) {
            otherMinhashes[i] = otherIt.next().hashValue;
        }

        // compute intersection between both sketches
        IntSet commonHashes = commonValues(thisMinhashes, otherMinhashes);
        if (commonHashes.isEmpty()) {
//            throw new IllegalArgumentException("No intersection between both skectches");
            return Double.NaN;
        }

        Int2DoubleMap thisMap = buildHashToValueMap(this.kMinValues);
        Int2DoubleMap otherMap = buildHashToValueMap(other.kMinValues);
        double[] thisValues = new double[commonHashes.size()];
        double[] otherValues = new double[commonHashes.size()];
        int i = 0;
        for (int hash : commonHashes) {
            // TODO: This should never happen if the algorithm is correct. Can be removed for performance.
            if (!thisMap.containsKey(hash)) {
                throw new IllegalStateException("common hash not found in 'this' map.");
            }
            thisValues[i] = thisMap.get(hash);
            otherValues[i] = otherMap.get(hash);
//            System.out.printf("thisValues[%d]=%.6f otherValues[%d]=%.6f\n", i, thisValues[i], i, otherValues[i]);
            i++;
        }
        // finally, compute correlation coefficient between common values
        return PearsonCorrelation.coefficient(thisValues, otherValues);
    }

    private Int2DoubleMap buildHashToValueMap(TreeSet<ValueHash> kMinValues) {
        Int2DoubleMap map = new Int2DoubleOpenHashMap();
        for (ValueHash vh : kMinValues) {
            map.putIfAbsent(vh.hashValue, vh.value);
        }
        return map;
    }

    private IntAVLTreeSet commonValues(int[] thisMinhashes, int[] otherMinhashes) {
        IntAVLTreeSet commonHashes = new IntAVLTreeSet(thisMinhashes);
        commonHashes.retainAll(new IntAVLTreeSet(otherMinhashes));
        return commonHashes;
    }

    public TreeSet<ValueHash> getKMinValues() {
        return this.kMinValues;
    }

    public static class ValueHash {

        private static final Comparator<ValueHash> COMPARATOR_ASC = new HashValueComparatorAscending();

        protected int hashValue;
        protected double grmHash;
        protected double value;

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
            return "ValueHash{" +
                    "hashValue=" + hashValue +
                    ", grmHash=" + grmHash +
                    ", value=" + value +
                    '}';
        }
    }

    private static class HashValueComparatorAscending implements Comparator<ValueHash> {

        @Override
        public int compare(ValueHash a, ValueHash b) {
            return Double.compare(a.grmHash, b.grmHash);
        }
    }

    @Override
    public String toString() {
        return "KMVCorrelationSketch{" +
                "k=" + k +
                ", kMinValues=" + kMinValues +
                ", cardinality=" + cardinality +
                ", largestGrmhash=" + largestGrmhash +
                '}';
    }
}
