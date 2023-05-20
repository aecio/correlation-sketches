package corrsketches.kmv;

import corrsketches.aggregations.AggregateFunction;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class PRISKTest {

  @Test
  public void shouldGiveHigherPriorityToItemsThatRepeatFrequentlyAndAggregateValues() {
    // given
    int[] keys = new int[] {6, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5};
    double[] values = new double[] {6.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 3.0, 4.0, 5.0};

    int maxSize = 3;
    // when
    PRISK sk =
        new PRISK.Builder()
            .aggregate(AggregateFunction.SUM)
            .maxSize(maxSize)
            .buildFromHashedKeys(keys, values);

    // then
    int[] sketchKeys = sk.getSamples().keys;
    double[] sketchValues = sk.getSamples().values;

    // keys should be unique due to aggregation
    assertThat(sk.getSamples().uniqueKeys).isTrue();
    // size must be as requested
    assertThat(sketchKeys.length).isEqualTo(maxSize);
    assertThat(sketchValues.length).isEqualTo(maxSize);

    // should include keys that repeat very frequently
    int indexOfOne = indexOf(sketchKeys, 1); // find the index of the key 1
    assertThat(indexOfOne).isBetween(0, keys.length);
    assertThat(sketchKeys[indexOfOne]).isEqualTo(1);
    assertThat(sketchValues[indexOfOne]).isEqualTo(7.0); // the sum of values of the key 1
  }

  @Test
  public void shouldGiveHigherPriorityToItemsThatRepeatFrequently() {
    // given
    int[] keys = new int[] {6, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5};
    double[] values = new double[] {6.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 3.0, 4.0, 5.0};

    int maxSize = 4;
    // when
    PRISK sk =
        new PRISK.Builder()
            .aggregate(AggregateFunction.NONE)
            .maxSize(maxSize)
            .buildFromHashedKeys(keys, values);

    // then
    int[] sketchKeys = sk.getSamples().keys;
    double[] sketchValues = sk.getSamples().values;

    // keys should not be unique as we're not using aggregation
    assertThat(sk.getSamples().uniqueKeys).isFalse();
    // size must be as requested
    assertThat(sketchKeys.length).isCloseTo(maxSize, Offset.offset(1));
    assertThat(sketchValues.length).isCloseTo(maxSize, Offset.offset(1));

//    System.out.println("sketchKeys = " + Arrays.toString(sketchKeys));
//    System.out.println("sketchValues = " + Arrays.toString(sketchValues));

    // should include keys that repeat very frequently
    int inclusions = 0;
    for (int i = 0; i < sketchKeys.length; i++) {
      if (sketchKeys[i] == 1) {
        inclusions++;
        assertThat(sketchValues[i]).isEqualTo(1.0);
      }
    }
    assertThat(inclusions).isGreaterThan(1);
  }

  public static int indexOf(int[] arr, int val) {
    return IntStream.range(0, arr.length).filter(i -> arr[i] == val).findFirst().orElse(-1);
  }
}
