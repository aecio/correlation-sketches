# Correlation Sketches

This repository contains the implementation of multiple methods for correlated dataset search using sketches.
It includes the code for creating correlation sketches, such as CSK (SIGMOD'21) and TUPSK (ICDE'24), and QCR indexes (ICDE'22).

## References

The technical description of these algorithms is available in the following papers.
Feel free to cite them if you use code from this repository.

> Santos, Aécio, Aline Bessa, Fernando Chirigati, Christopher Musco, and Juliana Freire. "Correlation sketches for approximate join-correlation queries." In Proceedings of the 2021 International Conference on Management of Data, pp. 1531-1544. 2021.

> Santos, Aécio, Aline Bessa, Christopher Musco, and Juliana Freire. "A sketch-based index for correlated dataset search." In 2022 IEEE 38th International Conference on Data Engineering (ICDE), pp. 2928-2941. IEEE, 2022.

> Santos, Aécio, Flip Korn, and Juliana Freire. "Efficiently Estimating Mutual Information Between Attributes Across Tables." In 2022 IEEE 40th International Conference on Data Engineering (ICDE), pp. 193-206. IEEE, 2024.


## API Usage

### Sketches

Correlation Sketches code is implemented in the class `corrsketches.CorrelationSketch`.
Below is an example of how to create sketches.
You can also find more tests and usage examples in the unit tests located in the class `corrsketches.correlation.CorrelationSketchTest`.

```java
List<String> kx = Arrays.asList("a", "a", "b", "b", "c", "d");
// sum: a=1 b=2 c=3 d=4, mean: a=0.5 b=1 c=3 d=4, count: a=2, c=2, c=1, d=1
double[] x = new double[] {-20., 21.0, 1.0, 1.0, 3.0, 4.0};

List<String> ky = Arrays.asList("a", "b", "c", "d");
double[] ysum = new double[] {1.0, 2.0, 3.0, 4.0};
double[] ymean = new double[] {0.5, 1.0, 3.0, 4.0};
double[] ycount = new double[] {2.0, 2.0, 1.0, 1.0};

final Builder builder = CorrelationSketch.builder().aggregateFunction(AggregateFunction.FIRST);

CorrelationSketch csySum = builder.build(ky, ysum);
CorrelationSketch csyMean = builder.build(ky, ymean);
CorrelationSketch csyCount = builder.build(ky, ycount);

CorrelationSketch csxSum = builder.aggregateFunction(AggregateFunction.SUM).build(kx, x);
CorrelationSketch csxMean = builder.aggregateFunction(AggregateFunction.MEAN).build(kx, x);
CorrelationSketch csxCount = builder.aggregateFunction(AggregateFunction.COUNT).build(kx, x);

double delta = 0.0001;
assertEquals(1.000, csxSum.correlationTo(csySum).value, delta);
assertEquals(1.000, csxMean.correlationTo(csyMean).value, delta);
assertEquals(1.000, csxCount.correlationTo(csyCount).value, delta);
```

### QCR Index

The QCR index implementation from the ICDE'21 paper is the class `corrsketches.benchmark.index.QCRSketchIndex`. 
Using the default constructor of `QCRSketchIndex`, will create an in-memory index and will construct sketches
using the default configurations. But you can also customize the constructor to store the index and to use 
different correlation estimators as in the example bellow.

To see more examples of how to use the API, you can look at the unit test class: `SketchIndexTest`.

```java
  // Creates data samples. createColumnPair() is a simple function that
  // instantiates the ColumnPair objects. Its implementation is available
  // in the class SketchIndexTest, but you just need to set the data to
  // ColumnPair objects as follows:
  //
  //   ColumnPair cp = new ColumnPair();
  //   cp.columnValues = columnValues;
  //   cp.keyValues = keyValues;
  //
  ColumnPair q = createColumnPair(
      Arrays.asList("a", "b", "c", "d", "e"),
      new double[] {1.0, 2.0, 3.0, 4.0, 5.0});

  ColumnPair c0 = createColumnPair(
      Arrays.asList("a", "b", "c", "d", "e"),
      new double[] {1.0, 2.0, 3.0, 4.0, 5.0});

  ColumnPair c1 = createColumnPair(
      Arrays.asList("a", "b", "c", "d"),
      new double[] {1.1, 2.5, 3.0, 4.4});

  ColumnPair c2 = createColumnPair(
      Arrays.asList("a", "b", "c"),
      new double[] {1.0, 3.1, 3.2});

  
  // The builder allows to customize the sketching method, correlation estimator, etc.
  CorrelationSketch.Builder builder = new CorrelationSketch.Builder()
      .aggregateFunction(AggregateFunction.MEAN)
      .estimator(CorrelationType.get((CorrelationType.PEARSONS)));
  boolean readonly = false;
  
  // sortBy determines the final re-ranking method after the retrieval using the QCR keys. 
  // - Sort.QCR orders hits by QCR key overlap.
  // - SortBy.CSK sorts using correlation sketches estimates. 
  SortBy sortBy = SortBy.KEY;

  // The path where to store the index. If null, an in-memory index will be created.
  String indexPath = null;
  
  // Initializes the index
  SketchIndex index = new QCRSketchIndex(indexPath, builder, sortBy, readonly );

  // Creates sketches and adds them to the index
  index.index("c0", c0);
  index.index("c1", c1);
  index.index("c2", c2);
  // sketches will appear on searches only after a refresh.
  index.refresh(); 

  // retrieve top-5 items for the query q
  List<Hit> hits = index.search(q, 5);

  System.out.println("Total hits: " + hits.size());
  for (int i = 0; i < hits.size(); i++) {
    Hit hit = hits.get(i);
    System.out.printf("\n[%d] ", i + 1);
    // the id used to index the sketch ("c0", "c1", etc)
    System.out.println("id: " + hit.id);
    // the keys overlap computed by the index processing
    System.out.println("    score: " + hit.score);
    // estimated using the sketches
    System.out.println("    correlation: " + hit.correlation());
  }
```


## Benchmarks

This repository has two main modules:
- `core` - Contains only the sketch implementations and correlation estimators.
- `benchmark` - Contains code for running benchmarks that were used in the papers.

The code for running the paper experiments is mainly in the submodule `benchmark`. 
This module depends on the module `core`, which contains the sketch implementations.

### Building and running the code

The project uses Gradle, so you can:

Compile and run the tests:

    ./gradlew check

You can also build a runnable package using:

    ./gradlew installDist

To run it, you can run the script `benchmark` that will be generated 
at the `benchmark/build/install/benchmark/bin` folder, i.e.,:

    ./benchmark/build/install/benchmark/bin/benchmark

To use this method, you will need to create your code somewhere in the project and register
it as a subcommand in the annotation `@Command` in the class `corrsketches.benchmark.Main`.
You can look at the `corrsketches.benchmark.IndexCorrelationBenchmark` for an example.


Alternatively, you can build a single jar containing all classes:

    ./gradle shadowJar

the JAR file will be created at: `benchmark/build/libs/benchmark-0.1-SNAPSHOT-all.jar`.
You can any class with a `main()` function using the standard `java -jar` command.
