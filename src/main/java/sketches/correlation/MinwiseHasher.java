package sketches.correlation;

import java.util.HashSet;
import java.util.Random;

/*
 * Generates a family of hash functions that can be used for locality-sensitive hashing (LSH).
 */
public class MinwiseHasher {

    public final int nextPrime = 2147483587;
    public final int maxValue = nextPrime - 1;
    public int[] coeffA;
    public int[] coeffB;
    public int numOfHashes;
    private int seed;

    /**
     * Creates a family of universal hash functions. Uses a fixed seed number (chosen randomly) to
     * generate the hash functions.
     *
     * @param numOfHashes The number of hash functions to generate.
     */
    public MinwiseHasher(int numOfHashes) {
        this(numOfHashes, 1947);
    }

    /**
     * Creates a family of universal hash functions.
     *
     * @param numOfHashes The number of hash functions to generate.
     * @param seed The seed number used to generate the hash functions.
     */
    public MinwiseHasher(int numOfHashes, int seed) {
        this.numOfHashes = numOfHashes;
        this.seed = seed;
        this.coeffA = pickRandCoefficients(numOfHashes);
        this.coeffB = pickRandCoefficients(numOfHashes);
    }

    public Signatures signature(Iterable<Integer> hashedShingles) {
        Signatures signatures = new Signatures(numOfHashes);
        for (int i = 0; i < numOfHashes; i++) {
            int minPosition = 0;
            int valueIdx = 0;
            int min = nextPrime + 1;
            for (int shingle : hashedShingles) {
                shingle = shingle % maxValue;
                int h = (coeffA[i] * shingle + coeffB[i]) % nextPrime;
//                shingle = Math.floorMod(shingle, maxValue);
//                int h = Math.floorMod((coeffA[i] * shingle + coeffB[i]), nextPrime);
                if (h < min) {
                    min = h;
                    minPosition = valueIdx;
                }
                valueIdx++;
            }
            signatures.set(i, min, minPosition);
        }
        return signatures;
    }

    private int[] pickRandCoefficients(int k) {
        int[] rands = new int[k];
        HashSet<Integer> seen = new HashSet<Integer>(k);
        Random random = new Random(seed);
        int i = 0;
        while (k > 0) {
            int randIndex = random.nextInt(maxValue);
            while (seen.contains(randIndex)) {
                randIndex = random.nextInt(maxValue);
            }
            rands[i] = randIndex;
            seen.add(randIndex);
            k = k - 1;
            i++;
        }
        return rands;
    }

    static class Signatures {

        protected final int size;
        protected final int[] hashes;
        protected final int[] positions;

        public Signatures(int size) {
            this.size = size;
            this.hashes = new int[size];
            this.positions = new int[size];
        }

        @Override
        public String toString() {
            return "Signatures{" + "hashes[" + this.hashes.length + "], positions="
                    + this.positions.length + '}';
        }

        public void set(int i, int hashValue, int position) {
            this.hashes[i] = hashValue;
            this.positions[i] = position;
        }
    }

}
