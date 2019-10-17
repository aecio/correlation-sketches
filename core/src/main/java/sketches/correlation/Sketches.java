package sketches.correlation;

import sketches.kmv.KMV;

public class Sketches {

  public enum Type {
    KMV,
    GKMV
  }

  public static KMVCorrelationSketch fromKmvStringHashedKeys(String[] hashes, double[] values) {
    if (hashes.length != values.length) {
      throw new IllegalArgumentException(
          "Number of values cannot be different from number of hashes");
    }
    KMV kmv = new KMV(hashes.length);
    for (int i = 0; i < hashes.length; i++) {
      kmv.update(Integer.parseInt(hashes[i]), values[i]);
    }
    return new KMVCorrelationSketch(kmv);
  }
}
