package corrsketches.benchmark.index;

import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.benchmark.ColumnPair;
import corrsketches.benchmark.IndexCorrelationBenchmark.SortBy;
import corrsketches.statistics.Stats;
import corrsketches.util.Hashes;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.TermQuery;

public class QCRISketchIndex extends SketchIndex {

  private static final String QCR_HASHES_FIELD_NAME = "c";
  private static final String QCR_OPPOSITE_HASHES_FIELD_NAME = "f";

  public QCRISketchIndex() throws IOException {
    super(null, new CorrelationSketch.Builder(), SortBy.KEY, false);
  }

  public QCRISketchIndex(
      String indexPath, CorrelationSketch.Builder builder, SortBy sort, boolean readonly)
      throws IOException {
    super(indexPath, builder, sort, readonly);
  }

  public void index(String id, ColumnPair columnPair) throws IOException {

    final CorrelationSketch sketch =
        super.builder.build(columnPair.keyValues, columnPair.columnValues);
    final ImmutableCorrelationSketch iSketch = sketch.toImmutable();

    final int[] keys = iSketch.getKeys();
    final double[] values = iSketch.getValues();

    // System.out.println(id);
    int[] indexKeys = computeCorrelationIndexKeys(keys, values);

    final double[] flippedValues = flip(values);
    int[] negIndexKeys = computeCorrelationIndexKeys(keys, flippedValues);

    Document doc = new Document();
    doc.add(new StringField(ID_FIELD_NAME, id, Field.Store.YES));
    for (int key : indexKeys) {
      doc.add(new StringField(QCR_HASHES_FIELD_NAME, intToBytesRef(key), Field.Store.NO));
    }
    for (int key : negIndexKeys) {
      doc.add(new StringField(QCR_OPPOSITE_HASHES_FIELD_NAME, intToBytesRef(key), Field.Store.NO));
    }

    // add keys to document
    for (int key : keys) {
      doc.add(new StringField(HASHES_FIELD_NAME, intToBytesRef(key), Field.Store.YES));
    }
    // add values to documents
    byte[] valuesBytes = toByteArray(values);
    doc.add(new StoredField(VALUES_FIELD_NAME, valuesBytes));

    writer.updateDocument(new Term(ID_FIELD_NAME, id), doc);
    //    refresh();
  }

  private static int[] computeCorrelationIndexKeys(int[] keys, double[] values) {
    double meanx = Stats.mean(values);
    double stdx = Stats.std(values);

    //    System.out.println("mean: " + meanx);

    int[] indexKeys = new int[keys.length];
    for (int i = 0; i < keys.length; i++) {
      final double q = (values[i] - meanx) / stdx;
      int sign = 0;
      if (q > 0.0) {
        sign = 1;
      } else if (q < 0.0) {
        sign = -1;
      }
      indexKeys[i] = Hashes.MURMUR3.newHasher().putInt(keys[i]).putInt(sign).hash().asInt();

      // DEBUG
      //      String signStr = "=";
      //      if (sign > 0) {
      //        signStr = "+";
      //      } else if (sign < 0) {
      //        signStr = "-";
      //      }
      //      System.out.printf("%d\t[%s]\t%s\n", keys[i], signStr, indexKeys[i]);
    }
    //    System.out.printf("\n");
    return indexKeys;
  }

  public List<Hit> search(ColumnPair columnPair, int k) throws IOException {

    CorrelationSketch query = builder.build(columnPair.keyValues, columnPair.columnValues);
    query.setCardinality(columnPair.keyValues.size());

    final ImmutableCorrelationSketch sketch = query.toImmutable();

    // System.out.println("q");
    int[] indexKeys = computeCorrelationIndexKeys(sketch.getKeys(), sketch.getValues());

    Builder bq1 = new Builder();
    Builder bq2 = new Builder();
    for (int i = 0; i < indexKeys.length; i++) {
      final int key = indexKeys[i];
      final Term term1 = new Term(QCR_HASHES_FIELD_NAME, intToBytesRef(key));
      bq1.add(new TermQuery(term1), Occur.SHOULD);
      final Term term2 = new Term(QCR_OPPOSITE_HASHES_FIELD_NAME, intToBytesRef(key));
      bq2.add(new TermQuery(term2), Occur.SHOULD);
    }
    // Query q = bq1.build();
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(Arrays.asList(bq1.build(), bq2.build()), 0f);

    return executeQuery(k, sketch, q);
  }

  private static double[] flip(double[] values) {
    final double[] flipped = new double[values.length];
    for (int i = 0; i < values.length; i++) {
      flipped[i] = -1 * values[i];
    }
    return flipped;
  }
}
