package hashtabledb;

public interface HashtableBackend {

  void putBytes(byte[] keyBytes, byte[] valueBytes);

  byte[] getBytes(byte[] keyBytes);

  void close();

  CloseableIterator<KV<byte[], byte[]>> createIterator();
}
