/*******************************************************************************
 * Copyright (c) 2010 Haifeng Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package corrsketches.util;

import corrsketches.statistics.DistanceFunction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import smile.sort.HeapSelect;

/**
 * This class implements the KD-tree data structure, which is can be used for search in
 * multidimensional data. KD-trees are typically used for answering k<sup>th</sup> nearest neighbor
 * search queries and range search queries in sets of vectors with low-to-moderate dimensionality.
 *
 * <p>This implementation is based on the KDTree class from the library smile v1.5.3 which was
 * licensed under the Apache License v2. The original code has been substantially modified.
 */
public class KDTree {

  /** The set of points being indexed in the KD-tree. */
  private final double[][] keys;
  /** The root node of the KD-Tree. */
  private final Node root;
  /** The index of each node. */
  private final int[] index;
  /** The default distance metric used to query the points. */
  private final Distance distance;

  /**
   * Constructor.
   *
   * @param key the set of points being indexed.
   * @param distance the default distance metric used to query the points
   */
  public KDTree(double[][] key, Distance distance) {
    this.keys = key;
    this.index = new int[key.length];
    this.distance = distance;
    for (int i = 0; i < key.length; i++) {
      index[i] = i;
    }
    root = buildTree(0, key.length);
  }

  /** Build a kd-tree from the given set of points. */
  private Node buildTree(int begin, int end) {

    Node node = new Node();
    node.count = end - begin;
    node.index = begin;

    // the number of dimensions
    int d = keys[0].length;

    // Calculate the upper and lower bounds for each dimension
    double[] lowerBound = new double[d];
    double[] upperBound = new double[d];
    for (int i = 0; i < d; i++) {
      lowerBound[i] = keys[index[begin]][i];
      upperBound[i] = keys[index[begin]][i];
    }
    for (int i = begin + 1; i < end; i++) {
      for (int j = 0; j < d; j++) {
        final double c = keys[index[i]][j];
        if (lowerBound[j] > c) {
          lowerBound[j] = c;
        }
        if (upperBound[j] < c) {
          upperBound[j] = c;
        }
      }
    }

    // Calculate bounding box stats
    double maxRadius = -1;
    for (int i = 0; i < d; i++) {
      double radius = (upperBound[i] - lowerBound[i]) / 2;
      if (radius > maxRadius) {
        maxRadius = radius;
        node.split = i;
        node.cutoff = (upperBound[i] + lowerBound[i]) / 2;
      }
    }

    // If the max spread is 0, make this a leaf node
    if (maxRadius == 0) {
      node.lower = node.upper = null;
      return node;
    }

    // Partition the dataset around the midpoint in this dimension. The
    // partitioning is done in-place by iterating from left-to-right and
    // right-to-left in the same way that partitioning is done in quicksort.
    int i1 = begin, i2 = end - 1, size = 0;
    while (i1 <= i2) {
      boolean i1Good = (keys[index[i1]][node.split] < node.cutoff);
      boolean i2Good = (keys[index[i2]][node.split] >= node.cutoff);

      if (!i1Good && !i2Good) {
        int temp = index[i1];
        index[i1] = index[i2];
        index[i2] = temp;
        i1Good = i2Good = true;
      }

      if (i1Good) {
        i1++;
        size++;
      }

      if (i2Good) {
        i2--;
      }
    }

    // Create the child nodes
    node.lower = buildTree(begin, begin + size);
    node.upper = buildTree(begin + size, end);

    return node;
  }

  /**
   * Returns the nearest neighbors of the given target starting from the give tree node.
   *
   * @param q the query key.
   * @param node the root of subtree.
   * @param neighbor the current nearest neighbor.
   */
  private void search(double[] q, Node node, Distance distanceFn, Neighbor neighbor) {
    if (node.isLeaf()) {
      // look at all the instances in this leaf
      for (int idx = node.index; idx < node.index + node.count; idx++) {
        // double distance = Math.squaredDistance(q, keys[index[idx]]);
        double distance = distanceFn.distance(q, keys[index[idx]]);
        if (distance < neighbor.distance) {
          neighbor.key = keys[index[idx]];
          neighbor.index = index[idx];
          neighbor.distance = distance;
        }
      }
    } else {
      Node nearer, further;
      double diff = q[node.split] - node.cutoff;
      if (diff < 0) {
        nearer = node.lower;
        further = node.upper;
      } else {
        nearer = node.upper;
        further = node.lower;
      }

      search(q, nearer, distanceFn, neighbor);

      // now look in further half
      if (neighbor.distance >= Math.abs(diff)) {
        search(q, further, distanceFn, neighbor);
      }
    }
  }

  /**
   * Returns (in the supplied heap object) the k nearest neighbors of the given target starting from
   * the given tree node.
   *
   * @param q the query key.
   * @param node the root of subtree.
   * @param heap the heap object to store/update the kNNs found during the search.
   */
  private void search(double[] q, Node node, Distance distanceFn, HeapSelect<Neighbor> heap) {
    if (node.isLeaf()) {
      // look at all the instances in this leaf
      for (int idx = node.index; idx < node.index + node.count; idx++) {
        // double distance = Math.squaredDistance(q, keys[index[idx]]);
        double distance = distanceFn.distance(q, keys[index[idx]]);

        Neighbor datum = heap.peek();
        if (distance < datum.distance) {
          datum.distance = distance;
          datum.index = index[idx];
          datum.key = keys[index[idx]];
          heap.heapify();
        }
      }
    } else {
      Node nearer, further;
      double diff = q[node.split] - node.cutoff;
      if (diff < 0) {
        nearer = node.lower;
        further = node.upper;
      } else {
        nearer = node.upper;
        further = node.lower;
      }

      search(q, nearer, distanceFn, heap);

      // now look in further half
      if (heap.peek().distance >= Math.abs(diff)) {
        search(q, further, distanceFn, heap);
      }
    }
  }

  /**
   * Returns the neighbors in the given range of search target from the given tree node.
   *
   * @param q the query key.
   * @param node the root of subtree.
   * @param radius the radius of search range from target.
   * @param neighbors the list of found neighbors in the range.
   */
  private void search(
      double[] q, Node node, double radius, Distance distanceFn, List<Neighbor> neighbors) {
    if (node.isLeaf()) {
      // look at all the instances in this leaf
      for (int idx = node.index; idx < node.index + node.count; idx++) {
        double distance = distanceFn.distance(q, keys[index[idx]]);
        if (distance <= radius) {
          neighbors.add(new Neighbor(keys[index[idx]], index[idx], distance));
        }
      }
    } else {
      Node nearer, further;
      double diff = q[node.split] - node.cutoff;
      if (diff < 0) {
        nearer = node.lower;
        further = node.upper;
      } else {
        nearer = node.upper;
        further = node.lower;
      }

      search(q, nearer, radius, distanceFn, neighbors);

      // now look in further half
      if (radius >= Math.abs(diff)) {
        search(q, further, radius, distanceFn, neighbors);
      }
    }
  }

  public Neighbor nearest(double[] q) {
    return nearest(q, this.distance);
  }

  public Neighbor nearest(double[] q, Distance distanceFn) {
    Neighbor neighbor = new Neighbor(null, 0, Double.MAX_VALUE);
    search(q, root, distanceFn, neighbor);
    neighbor.distance = Math.sqrt(neighbor.distance);
    return neighbor;
  }

  public Neighbor[] knn(double[] q, int k) {
    return knn(q, k, this.distance);
  }

  public Neighbor[] knn(double[] q, int k, Distance distanceFn) {
    if (k <= 0) {
      throw new IllegalArgumentException("Value of k must be at least 1, but got: " + k);
    }
    if (k > keys.length) {
      throw new IllegalArgumentException(
          "Number of nearest neighbors (k="
              + k
              + ") cannot be greater than the dataset size (n="
              + keys.length
              + ")");
    }

    Neighbor neighbor = new Neighbor(null, 0, Double.MAX_VALUE);
    Neighbor[] neighbors = new Neighbor[k];
    HeapSelect<Neighbor> heap = new HeapSelect<>(neighbors);
    for (int i = 0; i < k; i++) {
      heap.add(neighbor);
      neighbor = new Neighbor(null, 0, Double.MAX_VALUE);
    }

    search(q, root, distanceFn, heap);
    heap.sort();

    return neighbors;
  }

  public void range(double[] q, double radius, List<Neighbor> neighbors) {
    range(q, radius, this.distance, neighbors);
  }

  public void range(
      double[] q, double radius, Distance distanceFunction, List<Neighbor> neighbors) {
    if (radius <= 0.0) {
      throw new IllegalArgumentException("Invalid radius: " + radius);
    }
    search(q, root, radius, distanceFunction, neighbors);
  }

  public int countInRange(double[] q, double radius) {
    return countInRange(q, radius, this.distance);
  }

  public int countInRange(double[] q, double radius, Distance distanceFunction) {
    List<Neighbor> nn = new ArrayList<>();
    range(q, radius, distanceFunction, nn);
    return nn.size();
  }

  /** The distance metrics that can be used to query the kd-tree. */
  public enum Distance implements DistanceFunction {
    CHEBYSHEV(corrsketches.statistics.Distance::chebyshev),
    EUCLIDEAN(corrsketches.statistics.Distance::euclidean);

    private final DistanceFunction fn;

    Distance(DistanceFunction distanceFunction) {
      this.fn = distanceFunction;
    }

    @Override
    public double distance(double[] x, double[] y) {
      return fn.distance(x, y);
    }
  }

  /** The root in the KD-tree. */
  static class Node {

    /** Number of dataset stored in this node. */
    int count;
    /** The smallest point index stored in this node. */
    int index;
    /** The index of coordinate used to split this node. */
    int split;
    /** The cutoff used to split the specific coordinate. */
    double cutoff;
    /** The child node which values of split coordinate is less than the cutoff value. */
    Node lower;
    /**
     * The child node which values of split coordinate is greater than or equal to the cutoff value.
     */
    Node upper;

    /** If the node is a leaf node. */
    boolean isLeaf() {
      return lower == null && upper == null;
    }
  }

  public static class Neighbor implements Comparable<Neighbor> {

    /** The key of neighbor. */
    public double[] key;
    /** The index of neighbor object in the dataset. */
    public int index;
    /** The distance between the query and the neighbor. */
    public double distance;

    /**
     * Constructor.
     *
     * @param index the index of neighbor object in the dataset.
     * @param distance the distance between the query and the neighbor.
     */
    public Neighbor(double[] key, int index, double distance) {
      this.key = key;
      this.index = index;
      this.distance = distance;
    }

    @Override
    public int compareTo(Neighbor o) {
      int d = (int) Math.signum(distance - o.distance);
      // Sometimes, the dataset contains duplicate samples.
      // If the distances are same, we sort by the sample index.
      if (d == 0) {
        return index - o.index;
      } else {
        return d;
      }
    }

    @Override
    public String toString() {
      return "Neighbor{"
          + "key="
          + Arrays.toString(key)
          + ", index="
          + index
          + ", distance="
          + distance
          + '}';
    }
  }
}
