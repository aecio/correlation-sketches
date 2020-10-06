package corrsketches.benchmark.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

public class SetsTest {

  HashSet<String> keyA = new HashSet<>(Arrays.asList(new String[] {"a", "b", "c", "d", "e"}));
  HashSet<String> keyB = new HashSet<>(Arrays.asList(new String[] {"a", "b", "c", "z"}));
  HashSet<String> keyC = new HashSet<>(Arrays.asList(new String[] {"x", "y", "z"}));

  @Test
  public void shouldComputeIntersectionSize() {
    assertEquals(3, Sets.intersectionSize(keyA, keyB));
    assertEquals(3, Sets.intersectionSize(keyB, keyA));

    assertEquals(0, Sets.intersectionSize(keyA, keyC));
    assertEquals(0, Sets.intersectionSize(keyC, keyA));

    assertEquals(1, Sets.intersectionSize(keyB, keyC));
    assertEquals(1, Sets.intersectionSize(keyC, keyB));
  }

  @Test
  public void shouldComputeUnionSize() {
    assertEquals(6, Sets.unionSize(keyA, keyB));
    assertEquals(6, Sets.unionSize(keyB, keyA));

    assertEquals(8, Sets.unionSize(keyA, keyC));
    assertEquals(8, Sets.unionSize(keyC, keyA));

    assertEquals(6, Sets.unionSize(keyB, keyC));
    assertEquals(6, Sets.unionSize(keyC, keyB));
  }
}
