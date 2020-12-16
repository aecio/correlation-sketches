package corrsketches.benchmark.index;

import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.SketchType;
import corrsketches.benchmark.ColumnPair;
import corrsketches.correlation.Correlation.Estimate;
import corrsketches.correlation.CorrelationType;
import corrsketches.correlation.PearsonCorrelation;
import corrsketches.kmv.ValueHash;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;

public class SketchIndex {

  private static final String HASHES_FIELD_NAME = "h";
  private static final String VALUES_FIELD_NAME = "v";
  private static final String ID_FIELD_NAME = "i";

  private final IndexWriter writer;
  private final SearcherManager searcherManager;
  private final CorrelationSketch.Builder builder;

  public SketchIndex() {
    this(SketchType.KMV, 256);
  }

  public SketchIndex(String indexPath, SketchType sketchType, double threshold) throws IOException {
    this(MMapDirectory.open(Paths.get(indexPath)), sketchType, threshold);
  }

  @SuppressWarnings("deprecation")
  public SketchIndex(SketchType sketchType, double threshold) {
    this(new RAMDirectory(), sketchType, threshold);
  }

  public SketchIndex(Directory dir, SketchType sketchType, double threshold) {
    this.builder = CorrelationSketch.builder().sketchType(sketchType, threshold);
    final Analyzer analyzer = new StandardAnalyzer();
    final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
    iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);

    final BooleanSimilarity similarity = new BooleanSimilarity();
    iwc.setSimilarity(similarity);
    iwc.setRAMBufferSizeMB(256.0);
    try {
      this.writer = new IndexWriter(dir, iwc);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create index writer", e);
    }

    try {
      SearcherFactory searcherFactory =
          new SearcherFactory() {
            public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader) {
              IndexSearcher is = new IndexSearcher(reader);
              is.setSimilarity(similarity);
              return is;
            }
          };
      this.searcherManager = new SearcherManager(writer, searcherFactory);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create search manager", e);
    }
  }

  public void close() throws IOException {
    this.writer.close();
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
    writer.flush();
    searcherManager.maybeRefresh();
  }

  public List<Hit> search(ColumnPair columnPair, int k) throws IOException {

    CorrelationSketch query = builder.build(columnPair.keyValues, columnPair.columnValues);
    query.setCardinality(columnPair.keyValues.size());

    IndexSearcher searcher = searcherManager.acquire();
    try {
      Builder bq = new BooleanQuery.Builder();
      TreeSet<ValueHash> kMinValues = query.getKMinValues();
      for (ValueHash vh : kMinValues) {
        final Term term = new Term(HASHES_FIELD_NAME, intToBytesRef(vh.keyHash));
        bq.add(new TermQuery(term), Occur.SHOULD);
      }
      // bq.setMinimumNumberShouldMatch(1);

      TopDocs hits = searcher.search(bq.build(), k);

      ImmutableCorrelationSketch immutable = query.toImmutable();
      List<Hit> results = new ArrayList<>();
      for (int i = 0; i < hits.scoreDocs.length; i++) {
        final ScoreDoc scoreDoc = hits.scoreDocs[i];
        final Hit hit = createSearchHit(immutable, searcher.doc(scoreDoc.doc), scoreDoc.score);
        results.add(hit);
      }

      results.sort((a, b) -> Double.compare(b.correlation(), a.correlation()));

      return results;
    } finally {
      searcherManager.release(searcher);
    }
  }

  private Hit createSearchHit(ImmutableCorrelationSketch query, Document doc, float score) {

    // retrieve data from index fields
    String id = doc.getValues(ID_FIELD_NAME)[0];
    BytesRef[] hashesRef = doc.getBinaryValues(HASHES_FIELD_NAME);
    BytesRef[] valuesRef = doc.getBinaryValues(VALUES_FIELD_NAME);

    // re-construct sketch data structures from bytes
    ImmutableCorrelationSketch hitSketch = deserializeCorrelationSketch(hashesRef, valuesRef);
    return new Hit(id, query, hitSketch, score);
  }

  private static ImmutableCorrelationSketch deserializeCorrelationSketch(
      BytesRef[] hashesBytes, BytesRef[] valuesRef) {
    int[] hashes = bytesRefToIntArray(hashesBytes);
    double[] values = toDoubleArray(valuesRef[0].bytes);
    return new ImmutableCorrelationSketch(hashes, values, PearsonCorrelation::estimate);
  }

  private static int[] bytesRefToIntArray(BytesRef[] hashesBytes) {
    int[] hashes = new int[hashesBytes.length];
    for (int i = 0; i < hashes.length; i++) {
      hashes[i] = bytesRefToInt(hashesBytes[i]);
    }
    return hashes;
  }

  private static BytesRef intToBytesRef(int value) {
    byte[] bytes =
        new byte[] {
          (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) (value)
        };
    return new BytesRef(bytes);
  }

  private static int bytesRefToInt(BytesRef bytesRef) {
    final byte[] bytes = bytesRef.bytes;
    return ((bytes[0] & 0xFF) << 24)
        | ((bytes[1] & 0xFF) << 16)
        | ((bytes[2] & 0xFF) << 8)
        | ((bytes[3] & 0xFF) << 0);
  }

  protected static byte[] toByteArray(double[] value) {
    byte[] bytes = new byte[8 * value.length];
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    for (double v : value) {
      bb.putDouble(v);
    }
    return bytes;
  }

  protected static double[] toDoubleArray(byte[] bytes) {
    int n = bytes.length / 8;
    double[] doubles = new double[n];
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    for (int i = 0; i < n; i++) {
      doubles[i] = bb.getDouble();
    }
    return doubles;
  }

  public static class Hit {

    public final String id;
    public final float score;
    private final ImmutableCorrelationSketch query;
    private final ImmutableCorrelationSketch hit;
    private Estimate correlation;

    public Hit(
        String id,
        ImmutableCorrelationSketch query,
        ImmutableCorrelationSketch sketch,
        float score) {
      this.id = id;
      this.query = query;
      this.hit = sketch;
      this.score = score;
    }

    //    public double containment() {
    //      return query.containment(hit);
    //    }

    public double correlation() {
      if (this.correlation == null) {
        this.correlation = query.correlationTo(hit);
      }
      return correlation.coefficient;
    }

    public double robustCorrelation() {
      return query.correlationTo(hit, CorrelationType.get(CorrelationType.ROBUST_QN)).coefficient;
    }
  }
}
