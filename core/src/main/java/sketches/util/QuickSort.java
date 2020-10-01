package sketches.util;

import smile.sort.SortUtils;

public class QuickSort {

  private static final int M = 7;
  private static int NSTACK = 64;

  /**
   * Besides sorting the array arr, the array brr will be also rearranged as the same order of arr.
   */
  public static void sort(int[] arr, double[] brr) {
    sort(arr, brr, arr.length);
  }

  /**
   * This is an efficient implementation Quick Sort algorithm without recursive. Besides sorting the
   * first n elements of array arr, the first n elements of array brr will be also rearranged as the
   * same order of arr.
   */
  public static void sort(int[] arr, double[] brr, int n) {
    int jstack = -1;
    int l = 0;
    int[] istack = new int[NSTACK];
    int ir = n - 1;

    int i, j, k, a;
    double b;
    for (; ; ) {
      if (ir - l < M) {
        for (j = l + 1; j <= ir; j++) {
          a = arr[j];
          b = brr[j];
          for (i = j - 1; i >= l; i--) {
            if (arr[i] <= a) {
              break;
            }
            arr[i + 1] = arr[i];
            brr[i + 1] = brr[i];
          }
          arr[i + 1] = a;
          brr[i + 1] = b;
        }
        if (jstack < 0) {
          break;
        }
        ir = istack[jstack--];
        l = istack[jstack--];
      } else {
        k = (l + ir) >> 1;
        SortUtils.swap(arr, k, l + 1);
        SortUtils.swap(brr, k, l + 1);
        if (arr[l] > arr[ir]) {
          SortUtils.swap(arr, l, ir);
          SortUtils.swap(brr, l, ir);
        }
        if (arr[l + 1] > arr[ir]) {
          SortUtils.swap(arr, l + 1, ir);
          SortUtils.swap(brr, l + 1, ir);
        }
        if (arr[l] > arr[l + 1]) {
          SortUtils.swap(arr, l, l + 1);
          SortUtils.swap(brr, l, l + 1);
        }
        i = l + 1;
        j = ir;
        a = arr[l + 1];
        b = brr[l + 1];
        for (; ; ) {
          do {
            i++;
          } while (arr[i] < a);
          do {
            j--;
          } while (arr[j] > a);
          if (j < i) {
            break;
          }
          SortUtils.swap(arr, i, j);
          SortUtils.swap(brr, i, j);
        }
        arr[l + 1] = arr[j];
        arr[j] = a;
        brr[l + 1] = brr[j];
        brr[j] = b;
        jstack += 2;

        if (jstack >= NSTACK) {
          throw new IllegalStateException("NSTACK too small in sort.");
        }

        if (ir - i + 1 >= j - l) {
          istack[jstack] = ir;
          istack[jstack - 1] = i;
          ir = j - 1;
        } else {
          istack[jstack] = j - 1;
          istack[jstack - 1] = l;
          l = i;
        }
      }
    }
  }
}
