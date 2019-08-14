package sketches.correlation;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class MurmurHasher {

    private static final HashFunction MURMUR3 = Hashing.murmur3_32();

    public IntArrayList hashes(List<String> attributeValues) {
        IntArrayList hashedTokens = new IntArrayList();
        for (String token : attributeValues) {
            hashedTokens.add(MURMUR3.hashString(token, StandardCharsets.UTF_8).asInt());
        }
        return hashedTokens;
    }

}
