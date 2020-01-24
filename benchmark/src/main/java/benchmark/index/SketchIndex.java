package benchmark.index;

import benchmark.ColumnPair;
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
import org.apache.lucene.document.Field.Store;
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
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import sketches.correlation.KMVCorrelationSketch;
import sketches.correlation.Sketches.Type;
import sketches.kmv.GKMV;
import sketches.kmv.IKMV;
import sketches.kmv.KMV;
import sketches.kmv.ValueHash;

public class SketchIndex {

  private static final String HASHES_FIELD_NAME = "h";
  private static final String VALUES_FIELD_NAME = "v";
  private static final String ID_FIELD_NAME = "i";

  private final IndexWriter writer;
  private final SearcherManager searcherManager;
  private final Type sketchType;

  private final double threshold;

  public SketchIndex() {
    this(Type.KMV, 256);
  }

  public SketchIndex(String indexPath, Type sketchType, double threshold) throws IOException {
    this(MMapDirectory.open(Paths.get(indexPath)), sketchType, threshold);
  }

  public SketchIndex(Type sketchType, double threshold) {
    this(new RAMDirectory(), sketchType, threshold);
  }

  public SketchIndex(Directory dir, Type sketchType, double threshold) {
    this.sketchType = sketchType;
    this.threshold = threshold;

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

  public void index(String id, ColumnPair columnPair) throws IOException {

    KMVCorrelationSketch sketch = createCorrelationSketch(columnPair);

    Document doc = new Document();

    Field idField = new StringField(ID_FIELD_NAME, id, Field.Store.YES);
    doc.add(idField);

    TreeSet<ValueHash> hashes = sketch.getKMinValues();
    for (ValueHash hash : hashes) {
      doc.add(new StringField(HASHES_FIELD_NAME, String.valueOf(hash.hashValue), Store.YES));
    }

    double[] values = hashes.stream().mapToDouble(v -> v.value).toArray();
    byte[] valuesBytes = toByteArray(values);
    doc.add(new StoredField(VALUES_FIELD_NAME, valuesBytes));

    //        System.out.println(Arrays.asList(doc.getValues(HASHES_FIELD_NAME)));
    //        System.out.println(Arrays.asList(doc.getValues(VALUES_FIELD_NAME)));
    writer.updateDocument(new Term(ID_FIELD_NAME, id), doc);
    writer.flush();
    searcherManager.maybeRefresh();
  }

  public List<Hit> search(ColumnPair columnPair, int k) throws IOException {

    KMVCorrelationSketch query = createCorrelationSketch(columnPair);

    IndexSearcher searcher = searcherManager.acquire();
    try {
      Builder bq = new BooleanQuery.Builder();
      TreeSet<ValueHash> kMinValues = query.getKMinValues();
      for (ValueHash vh : kMinValues) {
        bq.add(
            new TermQuery(new Term(HASHES_FIELD_NAME, String.valueOf(vh.hashValue))), Occur.SHOULD);
      }
      //            bq.setMinimumNumberShouldMatch(1);

      //            System.out.println("Query: " + bq.build());
      TopDocs hits = searcher.search(bq.build(), k);
      //            TopDocs hits = searcher.search(new MatchAllDocsQuery(), k);

      List<Hit> results = new ArrayList<>();
      //            System.out.println(hits.totalHits);
      for (int i = 0; i < hits.scoreDocs.length; i++) {
        //                System.out.println(hits.scoreDocs[i]);

        // retrieve data from index fields
        Document doc = searcher.doc(hits.scoreDocs[i].doc);
        String id = doc.getValues(ID_FIELD_NAME)[0];
        String[] hashes = doc.getValues(HASHES_FIELD_NAME);
        BytesRef[] valuesRef = doc.getBinaryValues(VALUES_FIELD_NAME);

        // re-construct data structures from bytes
        double[] values = toDoubleArray(valuesRef[0].bytes);
        KMVCorrelationSketch correlationSketch = createCorrelationSketch(hashes, values);
        results.add(new Hit(id, correlationSketch));
      }
      return results;
    } finally {
      searcherManager.release(searcher);
    }
  }

  private KMVCorrelationSketch createCorrelationSketch(String[] hashes, double[] values) {
    IKMV kmv;
    if (sketchType == Type.KMV) {
      kmv = KMV.fromStringHashedKeys(hashes, values);
    } else if (sketchType == Type.GKMV) {
      kmv = GKMV.fromStringHashedKeys(hashes, values, threshold);
    } else {
      throw new IllegalArgumentException("Not supported yet!");
    }
    return new KMVCorrelationSketch(kmv);
  }

  protected static byte[] toByteArray(double[] value) {
    byte[] bytes = new byte[8 * value.length];
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    for (int i = 0; i < value.length; i++) {
      bb.putDouble(value[i]);
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


  private KMVCorrelationSketch createCorrelationSketch(ColumnPair columnPair) {
    KMVCorrelationSketch sketch;
    if (sketchType == Type.KMV) {
      int k = (int) threshold;
      KMV kmv = KMV.create(columnPair.keyValues, columnPair.columnValues, k);
      sketch = new KMVCorrelationSketch(kmv);
    } else {
      double t = threshold;
      GKMV gkmv = GKMV.create(columnPair.keyValues, columnPair.columnValues, t);
      sketch = new KMVCorrelationSketch(gkmv);
    }
    return sketch;
  }

  public static class Hit {

    public final String id;
    public final KMVCorrelationSketch sketch;

    public Hit(String id, KMVCorrelationSketch sketch) {
      this.id = id;
      this.sketch = sketch;
    }
  }
}
