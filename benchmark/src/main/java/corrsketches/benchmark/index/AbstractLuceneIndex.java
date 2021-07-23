package corrsketches.benchmark.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;

public abstract class AbstractLuceneIndex {

  protected final IndexWriter writer;
  protected final SearcherManager searcherManager;

  public AbstractLuceneIndex(String indexPath) throws IOException {
    Directory dir;
    if (indexPath == null) {
      dir = new ByteBuffersDirectory();
    } else {
      dir = MMapDirectory.open(Paths.get(indexPath));
    }
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

  public void refresh() throws IOException {
    writer.flush();
    searcherManager.maybeRefresh();
  }

  public void close() throws IOException {
    this.writer.close();
  }

  protected static int[] bytesRefToIntArray(BytesRef[] hashesBytes) {
    int[] hashes = new int[hashesBytes.length];
    for (int i = 0; i < hashes.length; i++) {
      hashes[i] = bytesRefToInt(hashesBytes[i]);
    }
    return hashes;
  }

  protected static BytesRef intToBytesRef(int value) {
    byte[] bytes =
        new byte[] {
          (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) (value)
        };
    return new BytesRef(bytes);
  }

  protected static int bytesRefToInt(BytesRef bytesRef) {
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
}
