package corrsketches.benchmark.index;

import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.SketchType;
import corrsketches.benchmark.ColumnPair;
import corrsketches.correlation.PearsonCorrelation;
import corrsketches.kmv.ValueHash;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;

public class SketchIndex extends AbstractLuceneIndex {

  protected static final String HASHES_FIELD_NAME = "h";
  protected static final String VALUES_FIELD_NAME = "v";
  protected static final String ID_FIELD_NAME = "i";

  protected final CorrelationSketch.Builder builder;
  protected final boolean sort;

  public SketchIndex() throws IOException {
    this(SketchType.KMV, 256);
  }

  public SketchIndex(SketchType sketchType, double threshold) throws IOException {
    this(null, sketchType, threshold);
  }

  public SketchIndex(String indexPath, SketchType sketchType, double threshold) throws IOException {
    this(indexPath, CorrelationSketch.builder().sketchType(sketchType, threshold), true, false);
  }

  public SketchIndex(String indexPath, CorrelationSketch.Builder builder, boolean sort, boolean readonly)
      throws IOException {
    super(indexPath, readonly);
    this.builder = builder;
    this.sort = sort;
  }

  public void index(String id, ColumnPair columnPair) throws IOException {

    CorrelationSketch sketch = builder.build(columnPair.keyValues, columnPair.columnValues);

    Document doc = new Document();

    Field idField = new StringField(ID_FIELD_NAME, id, Field.Store.YES);
    doc.add(idField);

    final ImmutableCorrelationSketch immutable = sketch.toImmutable();

    // add keys to document
    final int[] keys = immutable.getKeys();
    for (int key : keys) {
      doc.add(new StringField(HASHES_FIELD_NAME, intToBytesRef(key), Field.Store.YES));
    }

    // add values to documents
    final double[] values = immutable.getValues();
    byte[] valuesBytes = toByteArray(values);
    doc.add(new StoredField(VALUES_FIELD_NAME, valuesBytes));

    writer.updateDocument(new Term(ID_FIELD_NAME, id), doc);
    //    refresh();
  }

  public List<Hit> search(ColumnPair columnPair, int k) throws IOException {

    CorrelationSketch querySketch = builder.build(columnPair.keyValues, columnPair.columnValues);
    querySketch.setCardinality(columnPair.keyValues.size());

    Builder bq = new BooleanQuery.Builder();
    TreeSet<ValueHash> kMinValues = querySketch.getKMinValues();
    for (ValueHash vh : kMinValues) {
      final Term term = new Term(HASHES_FIELD_NAME, intToBytesRef(vh.keyHash));
      bq.add(new TermQuery(term), Occur.SHOULD);
    }

    final boolean sort = true;
    return executeQuery(k, querySketch.toImmutable(), bq.build(), sort);
  }

  protected List<Hit> executeQuery(int k, ImmutableCorrelationSketch cs, Query query, boolean sort)
      throws IOException {
    IndexSearcher searcher = searcherManager.acquire();
    try {
      TopDocs hits = searcher.search(query, k);
      List<Hit> results = new ArrayList<>();
      for (int i = 0; i < hits.scoreDocs.length; i++) {
        final ScoreDoc scoreDoc = hits.scoreDocs[i];
        final Hit hit = createSearchHit(cs, searcher, scoreDoc, sort);
        results.add(hit);
      }
      if (sort) {
        results.sort((a, b) -> Double.compare(b.correlationAbsolute(), a.correlationAbsolute()));
      }
      return results;
    } finally {
      searcherManager.release(searcher);
    }
  }

  protected Hit createSearchHit(
      ImmutableCorrelationSketch query,
      IndexSearcher searcher,
      ScoreDoc scoreDoc,
      boolean loadSketch)
      throws IOException {
    Document doc = searcher.doc(scoreDoc.doc);
    // retrieve id from index fields
    String id = doc.getValues(ID_FIELD_NAME)[0];
    // read sketch from index fields
    ImmutableCorrelationSketch sketch = loadSketch ? readSketchFromIndex(doc) : null;
    return new Hit(id, query, sketch, scoreDoc.score, scoreDoc.doc, this);
  }

  protected ImmutableCorrelationSketch readSketchFromIndex(Document doc) {
    // retrieve data from index fields
    BytesRef[] hashesRef = doc.getBinaryValues(HASHES_FIELD_NAME);
    BytesRef[] valuesRef = doc.getBinaryValues(VALUES_FIELD_NAME);
    // re-construct sketch data structures from bytes
    int[] hashes = bytesRefToIntArray(hashesRef);
    double[] values = toDoubleArray(valuesRef[0].bytes);
    return new ImmutableCorrelationSketch(hashes, values, PearsonCorrelation::estimate);
  }

  ImmutableCorrelationSketch loadSketch(int docId) throws IOException {
    IndexSearcher searcher = searcherManager.acquire();
    try {
      Document doc = searcher.doc(docId);
      return readSketchFromIndex(doc);
    } finally {
      searcherManager.release(searcher);
    }
  }
}
