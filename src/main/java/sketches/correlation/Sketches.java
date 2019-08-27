package sketches.correlation;

public class Sketches {

    enum Type {
        KMV,
        MINHASH
    }

    private Type type;
    private int k;

    public Sketches(Type type, int k) {
        this.type = type;
        this.k = k;
    }

    public void build() {
        // TODO
    }

}
