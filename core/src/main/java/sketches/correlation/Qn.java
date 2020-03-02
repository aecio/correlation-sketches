package sketches.correlation;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.doubles.DoubleComparators;
import it.unimi.dsi.fastutil.doubles.DoubleHeapPriorityQueue;
import java.util.Arrays;
import smile.math.Math;

/**
 * Implements the scale estimator proposed in Peter J. Rousseeuw & Christophe Croux (1993)
 * Alternatives to the Median Absolute Deviation, Journal of the American Statistical Association,
 * 88:424, 1273-1283, DOI: 10.1080/01621459.1993.10476408.
 */
public class Qn {

  /**
   * Asymptotic consistence factor. The is value used here is deviates from the value used in the
   * original paper (2.2219), which seems to have a typo. The correction factors used in this class
   * are approximations consistent with the values used the in 'robustbase' R package.
   */
  public static final double GAUSSIAN_CONSISTENCY_FACTOR = 2.21914;

  private static final double SQRT_OF_TWO = Math.sqrt(2);

  /**
   * Implements the time-efficient algorithm for the Qn scale estimator proposed by Rousseeuw and
   * Croux. The algorithm implemented here runs in O(n log n) time and was originally proposed in:
   * Croux C., Rousseeuw P.J. (1992) Time-Efficient Algorithms for Two Highly Robust Estimators of
   * Scale. In: Dodge Y., Whittaker J. (eds) Computational Statistics. Physica, Heidelberg.
   *
   * @param x an array containing the observations
   * @return the Qn estimate
   */
  static double estimateScale(final double[] x) {
    checkArgument(x.length > 1, "array length must be at least 2, found %s", x.length);

    double Qn = Double.NaN;
    int n = x.length;

    int[] left = new int[n];
    int[] right = new int[n];
    int[] P = new int[n];
    int[] Q = new int[n];
    int[] weight = new int[n];
    double[] work = new double[n];

    // the following need to be a long to avoid overflow
    long k, knew, nL, nR, sumP, sumQ;

    int h = n / 2 + 1;
    k = ((long) h) * (h - 1) / 2;

    double[] y = Arrays.copyOf(x, x.length);
    Arrays.sort(y);

    for (int i = 0; i < n; i++) {
      left[i] = n - i + 1; // use + 1 instead of +2 because of 0-indexing
      right[i] = (i <= h) ? n : n - (i - h);
    }

    nL = ((long) n) * (n + 1) / 2;
    nR = ((long) n) * n;
    knew = k + nL;

    boolean found = false;
    double trial;
    int j, jhelp;

    while ((!found) && (nR - nL > n)) {
      j = 0;
      for (int i = 1; i < n; i++) {
        if (left[i] <= right[i]) {
          weight[j] = right[i] - left[i] + 1;
          jhelp = left[i] + weight[j] / 2;
          work[j] = (y[i] - y[n - jhelp]);
          j++;
        }
      }
      trial = weightedHighMedian(work, weight, j);

      j = 0;
      for (int i = n - 1; i >= 0; --i) {
        while ((j < n) && ((y[i] - y[n - j - 1]) < trial)) {
          j++;
        }
        P[i] = j;
      }

      j = n + 1;
      for (int i = 0; i < n; i++) {
        while ((y[i] - y[n - j + 1]) > trial) {
          j--;
        }
        Q[i] = j;
      }

      sumQ = 0;
      sumP = 0;

      for (int i = 0; i < n; i++) {
        sumP = sumP + P[i];
        sumQ = sumQ + Q[i] - 1;
      }

      if (knew <= sumP) {
        for (int i = 0; i < n; i++) {
          right[i] = P[i];
        }
        nR = sumP;
      } else if (knew > sumQ) {
        for (int i = 0; i < n; i++) {
          left[i] = Q[i];
        }
        nL = sumQ;
      } else {
        Qn = trial;
        found = true;
      }
    }
    if (!found) {
      j = 0;
      for (int i = 1; i < n; i++) {
        if (left[i] <= right[i]) {
          for (int jj = left[i]; jj <= right[i]; ++jj) {
            work[j] = y[i] - y[n - jj];
            j++;
          }
        }
      }
      Qn = findKthOrderStatistic(work, j, (int) (knew - nL));
    }

    /* Corrections are consistent with the implementation of the 'robustbase' R package */
    double dn = 1.0;
    if (n <= 12) {
      if (n == 2) {
        dn = 0.399356;
      } else if (n == 3) {
        dn = 0.99365;
      } else if (n == 4) {
        dn = 0.51321;
      } else if (n == 5) {
        dn = 0.84401;
      } else if (n == 6) {
        dn = 0.61220;
      } else if (n == 7) {
        dn = 0.85877;
      } else if (n == 8) {
        dn = 0.66993;
      } else if (n == 9) {
        dn = 0.87344;
      } else if (n == 10) {
        dn = .72014;
      } else if (n == 11) {
        dn = .88906;
      } else if (n == 12) {
        dn = .75743;
      }
    } else {
      if (n % 2 == 1) {
        // n odd
        dn = 1.60188 + (-2.1284 - 5.172 / n) / n;
      } else {
        // n even
        dn = 3.67561 + (1.9654 + (6.987 - 77.0 / n) / n) / n;
      }
      dn = 1.0 / (dn / (double) n + 1.0);
    }

    double correctedQn = Qn * dn * GAUSSIAN_CONSISTENCY_FACTOR;
    return correctedQn;
  }

  public QnEstimate estimateScaleWithError(double[] x) {
    double qn = this.estimateScale(x);
    double error = qn / Math.sqrt(2.0 * (x.length - 1) * 0.8227);
    return new QnEstimate(qn, error);
  }

  /**
   * Algorithm to compute the weighted high median in O(n) time.
   *
   * <p>The Weighted High Median (whimed) is defined as the smallest a(j) such that the sum of the
   * weights of all a(i) <= a(j) is strictly greater than half of the total weight.
   *
   * @param a real array containing the observations
   * @param iw array of integer weights of the observations.
   * @param n number of observations
   */
  static double weightedHighMedian(double[] a, int[] iw, int n) {
    int kcand;
    double[] acand = new double[n];
    int[] iwcand = new int[n];

    // these need to be a long to avoid overflow
    long wleft, wmid, wright, wtotal, wrest;
    double trial;

    int nn = n;
    wtotal = 0;
    for (int i = 0; i < nn; i++) {
      wtotal += iw[i];
    }

    wrest = 0;
    while (true) {
      trial = findKthOrderStatistic(a, nn, nn / 2);

      wleft = 0;
      wmid = 0;
      wright = 0;

      for (int i = 0; i < nn; i++) {
        if (a[i] < trial) {
          wleft += iw[i];
        } else if (a[i] > trial) {
          wright += iw[i];
        } else {
          wmid += iw[i];
        }
      }

      kcand = 0;
      if (2 * (wrest + wleft) > wtotal) {
        for (int i = 0; i < nn; i++) {
          if (a[i] < trial) {
            acand[kcand] = a[i];
            iwcand[kcand] = iw[i];
            kcand++;
          }
        }
        nn = kcand;
      } else if (2 * (wrest + wleft + wmid) <= wtotal) {
        for (int i = 0; i < nn; i++) {
          if (a[i] > trial) {
            acand[kcand] = a[i];
            iwcand[kcand] = iw[i];
            kcand++;
          }
        }
        nn = kcand;
        wrest += wleft + wmid;
      } else {
        return trial;
      }
      for (int i = 0; i < nn; i++) {
        a[i] = acand[i];
        iw[i] = iwcand[i];
      }
    }
  }

  /** Finds the k-th order statistic of an array array a of length n. */
  static double findKthOrderStatistic(double[] x, int n, int k) {
    // Check if arguments are valid
    final int N = x.length;
    checkArgument(n <= N, "n=[%s] can't be greater than the length of array x=[%s]", n, N);
    checkArgument(k <= n, "k=[%s] can't be greater than n", k, n);
    checkArgument(k <= N, "k=[%s] can't be greater than the length of array x=[%s]", k, N);
    if (n == 1) {
      return x[0];
    }
    return heapSelect(x, n, k);
  }

  static double heapSelect(double[] x, int n, int k) {
    DoubleHeapPriorityQueue q =
        new DoubleHeapPriorityQueue(k, DoubleComparators.OPPOSITE_COMPARATOR);
    for (int i = 0; i < n; i++) {
      if (q.size() < k) {
        q.enqueue(x[i]);
      } else {
        q.enqueue(x[i]);
        q.dequeueDouble();
      }
    }
    return q.firstDouble();
  }

  public static double correlation(double[] x, double[] y) {
    Preconditions.checkArgument(x.length == y.length, "x and y dimensions must match");

    double stdx = estimateScale(x);
    double stdy = estimateScale(y);

    int n = y.length;
    double[] u = new double[n];
    double[] v = new double[n];

    for (int i = 0; i < n; i++) {
      double xstdsqrt2 = (x[i] / stdx) / SQRT_OF_TWO;
      double ystdsqrt2 = (y[i] / stdy) / SQRT_OF_TWO;
      u[i] = xstdsqrt2 + ystdsqrt2;
      v[i] = xstdsqrt2 - ystdsqrt2;
    }

    double uscale = estimateScale(u);
    double vscale = estimateScale(v);

    double us2 = uscale * uscale;
    double vs2 = vscale * vscale;

    return (us2 - vs2) / (us2 + vs2);
  }

  static class QnEstimate {

    final double correctedQn;
    final double correctedQnError;

    public QnEstimate(double correctedQn, double correctedQnError) {
      this.correctedQn = correctedQn;
      this.correctedQnError = correctedQnError;
    }
  }
}
