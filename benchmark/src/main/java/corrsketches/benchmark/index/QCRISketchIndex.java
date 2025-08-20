package corrsketches.benchmark.index;

import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.benchmark.ColumnPair;
import corrsketches.statistics.Stats;
import corrsketches.util.Hashes;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
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

    final ImmutableCorrelationSketch sketch =
        super.builder
            .build(columnPair.keyValues, columnPair.columnValues, columnPair.columnValueType)
            .toImmutable();

    final int[] keys = sketch.getKeys();
    final double[] values = sketch.getValues();

    Document doc = new Document();
    doc.add(new StringField(ID_FIELD_NAME, id, Field.Store.YES));

    int[] indexKeys = computeCorrelationIndexKeys(keys, values);
    int[] negIndexKeys = computeCorrelationIndexKeys(keys, flip(values));

    // store and index sketch data in the document
    indexIntArray(doc, QCR_HASHES_FIELD_NAME, indexKeys);
    indexIntArray(doc, QCR_OPPOSITE_HASHES_FIELD_NAME, negIndexKeys);
    indexAndStoreIntArray(doc, HASHES_FIELD_NAME, keys);
    storeDoubleArray(doc, VALUES_FIELD_NAME, values);
    storeInt(doc, VALUES_TYPE_FIELD_NAME, sketch.valuesType().intValue);

    writer.updateDocument(new Term(ID_FIELD_NAME, id), doc);
  }

  private static int[] computeCorrelationIndexKeys(int[] keys, double[] values) {
    double meanx = Stats.mean(values);
    double stdx = Stats.std(values);

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
    }
    return indexKeys;
  }

  public List<Hit> search(ColumnPair columnPair, int k) throws IOException {

    CorrelationSketch query =
        builder.build(columnPair.keyValues, columnPair.columnValues, columnPair.columnValueType);
    query.setCardinality(columnPair.keyValues.size());

    final ImmutableCorrelationSketch sketch = query.toImmutable();

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
