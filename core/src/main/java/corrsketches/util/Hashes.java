package corrsketches.util;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Hashes {

  private static final HashFunction MURMUR3 = Hashing.murmur3_32();

  /**
   * The inverse golden ratio as a fraction. This has more precision than using the formula:
   * (Math.sqrt(5.0) - 1.0) / 2.0.
   */
  private static final double INVERSE_GOLDEN_RATIO =
      0.6180339887498949025; // the inverse golden ratio

  //    private static double GOLDEN_RATIO = (1 + Math.sqrt(5)) / 2;
  private static double GOLDEN_RATIO = INVERSE_GOLDEN_RATIO + 1;

  /**
   * Computes the 32-bits murmur3 hash functions of all given values.
   *
   * @param values
   * @return a list of hashes of the given values
   */
  public IntArrayList murmur3_32(List<String> values) {
    IntArrayList hashes = new IntArrayList();
    for (String value : values) {
      hashes.add(MURMUR3.hashString(value, StandardCharsets.UTF_8).asInt());
    }
    return hashes;
  }

  /**
   * Computes the 32-bits murmur3 hash functions of the given value.
   *
   * @param value
   * @return hash of the given value
   */
  public static int murmur3_32(String value) {
    return MURMUR3.hashString(value, StandardCharsets.UTF_8).asInt();
  }

  /**
   * Transforms a integer into a double in the rage [0, 1] using the golden ratio multiplicative
   * hash function.
   */
  public static double grm(int hash) {
    final double h = (hash + 1d) * GOLDEN_RATIO;
    return h - Math.floor(h);
  }
}
