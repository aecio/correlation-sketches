package corrsketches.kmv;

import org.junit.jupiter.api.Test;

public class WPRISKTest {

  @Test
  public void shouldComputeWeightesCorrectly() {
//    int[] keys = new int[] {1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4};
//    double[] values = new double[] {1, 1, 1, 2, 1, 2, 1, 1, 2, 3, 4};
    int[] keys = new int[] {1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4};
    double[] values = new double[] {1, 4, 3, 5, 8, 9, 10, 7, 6, 3, 4};

    WPRISK sk = new WPRISK.Builder().maxSize(3).buildFromHashedKeys(keys, values);
    sk.estimateMutualInfo();
  }
}
