package corrsketches.statistics;

import static java.lang.Math.abs;

public class NearestNeighbors1D {

  public static int countPointsInRange(double[] data, double min, double max) {
    return (int) Math.floor(findPoint(data, max) - findPoint(data, min));
  }

  public static int countPointsInRange(double[] data, int dataEnd, double min, double max) {
    return (int) Math.floor(findPoint(data, dataEnd, max) - findPoint(data, dataEnd, min));
  }

  public static double findPoint(double[] c, double target) {
    return findPoint(c, c.length, target);
  }

  public static double findPoint(double[] c, int cEnd, double target) {
    int left = 1;
    int right = cEnd;

    if (target < c[left - 1]) {
      return 0.5;
    } else if (target > c[right - 1]) {
      return right + 0.5;
    }

    double pt = -1;
    while (left != right) {
      pt = (left + right) / 2;
      if (c[(int) pt - 1] < target) {
        left = (int) pt;
      } else {
        right = (int) pt;
      }
      if (left + 1 == right) {
        if (c[left - 1] == target) {
          pt = left;
        } else if (c[right - 1] == target) {
          pt = right;
        } else {
          pt = (right + left) / 2d;
        }
        break;
      }
    }
    return pt;
  }

  public static NearestNeighbor kthNearest(double[] data, final int target, int k) {
    NearestNeighbor nn = new NearestNeighbor();
    kthNearest(data, target, k, nn);
    return nn;
  }

  public static void kthNearest(double[] data, int target, int k, NearestNeighbor nn) {
    kthNearest(data, data.length, target, k, nn);
  }

  public static void kthNearest(double[] data, int dataEnd, int target, int k, NearestNeighbor nn) {
    // k must be at most the size of the input minus 1
    final int maxK = dataEnd - 1;
    int localK = Math.min(k, maxK);
    double c = data[target];

    int left = target;
    int right = target;
    int theNeighbor = -1; // initial value not used, but needed to make compiler happy

    for (int i = 0; i < localK; i++) {
      if (left == 0) {
        right++;
        theNeighbor = right;
      } else if (right == dataEnd - 1) {
        left--;
        theNeighbor = left;
      } else if (abs(data[left - 1] - c) < abs(data[right + 1] - c)) {
        left--;
        theNeighbor = left;
      } else {
        right++;
        theNeighbor = right;
      }
    }

    nn.k = localK;
    nn.distance = abs(data[theNeighbor] - c);
    nn.kthNearest = data[theNeighbor];
    nn.left = (theNeighbor == left);
  }

  public static NearestNeighbor kthNearestNonZero(double[] data, final int target, int k) {
    NearestNeighbor nn = new NearestNeighbor();
    kthNearestNonZero(data, target, k, nn);
    return nn;
  }

  public static void kthNearestNonZero(double[] data, int target, int k, NearestNeighbor nn) {
    kthNearestNonZero(data, data.length, target, k, nn);
  }

  public static void kthNearestNonZero(
      double[] data, int dataEnd, int target, int k, NearestNeighbor nn) {
    // k must be at most the size of the input minus 1
    final int maxK = dataEnd - 1;
    int localK = Math.min(k, maxK);

    double c = data[target];
    int left = target;
    int right = target;
    int theNeighbor = -1; // initial value not used, but needed to make compiler happy

    for (int i = 0; i < localK; i++) {
      if (left == 0) {
        right++;
        theNeighbor = right;
      } else if (right == dataEnd - 1) {
        left--;
        theNeighbor = left;
      } else if (abs(data[left - 1] - c) < abs(data[right + 1] - c)) {
        left--;
        theNeighbor = left;
      } else {
        right++;
        theNeighbor = right;
      }
    }
    double distance = abs(data[theNeighbor] - c);
    if (distance == 0) {
      while (localK < maxK && distance == 0) {
        if (left == 0) {
          right++;
          theNeighbor = right;
        } else if (right == data.length - 1) {
          left--;
          theNeighbor = left;
        } else if (abs(data[left - 1] - c) < abs(data[right + 1] - c)) {
          left--;
          theNeighbor = left;
        } else {
          right++;
          theNeighbor = right;
        }
        localK++;
        distance = abs(data[theNeighbor] - c);
      }
    }
    nn.k = localK;
    nn.distance = distance;
    nn.kthNearest = data[theNeighbor];
    nn.left = (theNeighbor == left);
  }

  public static class NearestNeighbor {

    public int k;
    public double kthNearest;
    public double distance;
    public boolean left;
  }
}
