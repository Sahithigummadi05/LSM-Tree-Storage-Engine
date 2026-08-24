package com.sahithi.lsm.bloom;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.BitSet;

/**
 * A probabilistic "is this key definitely absent?" test, used to skip reading SSTable files that
 * cannot contain a key.
 *
 * <p>Reads in an LSM tree are the expensive direction: a key may live in any of several on-disk
 * files, so a naive lookup touches every one. A bloom filter makes most of those visits
 * unnecessary — it answers "definitely not here" cheaply, from memory.
 *
 * <p><b>The asymmetry is the whole point, and it is not symmetric in danger.</b> A false positive
 * says a key might be present when it isn't: the engine reads a file for nothing, wasting I/O but
 * returning the correct answer. A false <i>negative</i> would say a key is absent when it is
 * actually stored — the engine would skip the file holding it and report data loss on a key that
 * was durably written. False positives are a performance cost; false negatives would be a
 * correctness catastrophe. The structure guarantees they cannot happen: bits are only ever set,
 * never cleared, so every key that was added still has all of its bits set.
 *
 * <p>Uses double hashing (Kirsch–Mitzenmacher): {@code h_i = h1 + i*h2} generates k independent
 * hashes from two, which is standard practice and avoids computing k separate digests per key.
 */
public final class BloomFilter {

    private final BitSet bits;
    private final int bitCount;
    private final int hashCount;

    private BloomFilter(BitSet bits, int bitCount, int hashCount) {
        this.bits = bits;
        this.bitCount = bitCount;
        this.hashCount = hashCount;
    }

    /**
     * Sizes a filter for an expected number of entries and a target false-positive rate, using the
     * standard optimal formulas: m = -n·ln(p)/ln(2)² bits, k = (m/n)·ln2 hashes.
     */
    public static BloomFilter create(int expectedEntries, double falsePositiveRate) {
        if (expectedEntries <= 0) {
            throw new IllegalArgumentException("expectedEntries must be positive");
        }
        if (falsePositiveRate <= 0 || falsePositiveRate >= 1) {
            throw new IllegalArgumentException("falsePositiveRate must be in (0, 1)");
        }
        var ln2Squared = Math.log(2) * Math.log(2);
        var bitCount = (int) Math.ceil(-expectedEntries * Math.log(falsePositiveRate) / ln2Squared);
        var hashCount = Math.max(1, (int) Math.round((double) bitCount / expectedEntries * Math.log(2)));
        return new BloomFilter(new BitSet(bitCount), bitCount, hashCount);
    }

    public void add(byte[] key) {
        var h1 = hash1(key);
        var h2 = hash2(key);
        for (var i = 0; i < hashCount; i++) {
            bits.set(bitIndex(h1, h2, i));
        }
    }

    /**
     * @return false if the key is <b>definitely</b> absent; true if it <i>might</i> be present.
     *     Never returns false for a key that was added.
     */
    public boolean mightContain(byte[] key) {
        var h1 = hash1(key);
        var h2 = hash2(key);
        for (var i = 0; i < hashCount; i++) {
            if (!bits.get(bitIndex(h1, h2, i))) {
                return false;
            }
        }
        return true;
    }

    private int bitIndex(int h1, int h2, int i) {
        // Math.floorMod rather than % because the combined hash can be negative, and a negative
        // index would throw rather than wrap - an easy way to turn a hash collision into a crash.
        return Math.floorMod(h1 + i * h2, bitCount);
    }

    private static int hash1(byte[] key) {
        var hash = 0x811c9dc5; // FNV-1a 32-bit offset basis
        for (var b : key) {
            hash ^= (b & 0xff);
            hash *= 0x01000193;
        }
        return hash;
    }

    private static int hash2(byte[] key) {
        // A different mixing function so the two hashes are independent enough for double hashing.
        var hash = 0;
        for (var b : key) {
            hash = 31 * hash + (b & 0xff);
        }
        // Force odd so h2 is coprime with common bit counts, keeping the probe sequence spread out.
        return (hash * 2 + 1);
    }

    public int bitCount() {
        return bitCount;
    }

    public int hashCount() {
        return hashCount;
    }

    // ------------------------------------------------------------------
    // Persistence - the filter is written alongside its SSTable and reloaded with it
    // ------------------------------------------------------------------

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(bitCount);
        out.writeInt(hashCount);
        var words = bits.toLongArray();
        out.writeInt(words.length);
        for (var word : words) {
            out.writeLong(word);
        }
    }

    public static BloomFilter readFrom(DataInputStream in) throws IOException {
        var bitCount = in.readInt();
        var hashCount = in.readInt();
        var wordCount = in.readInt();
        var words = new long[wordCount];
        for (var i = 0; i < wordCount; i++) {
            words[i] = in.readLong();
        }
        return new BloomFilter(BitSet.valueOf(words), bitCount, hashCount);
    }
}
