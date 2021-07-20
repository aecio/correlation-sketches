package hashtabledb;

public class BytesBytesHashtable extends AbstractDbHashtable
    implements Iterable<KV<byte[], byte[]>> {

  public BytesBytesHashtable(DBType backend, String path) {
    super(backend, path, false);
  }

  public BytesBytesHashtable(DBType backend, String path, boolean readonly) {
    super(backend, path, readonly);
  }

  public void put(byte[] key, byte[] value) {
    putBytes(key, value);
  }

  public byte[] get(byte[] key) {
    return getBytes(key);
  }

  @Override
  public CloseableIterator<KV<byte[], byte[]>> iterator() {
    return super.createIterator();
  }
}
