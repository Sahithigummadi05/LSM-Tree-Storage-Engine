package com.sahithi.lsm.bloom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The bloom filter's two error modes are not equally dangerous, and the tests reflect that.
 *
 * <p>A false positive costs a wasted file read and returns the right answer anyway. A false
 * negative would make the engine skip the file holding a key and report it missing — silent data
 * loss on data that was durably written. So the no-false-negatives property is tested
 * exhaustively, while the false-positive rate is only checked for being in a sane range.
 */
class BloomFilterTest {

    private static byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("every added key is reported as possibly present - no false negatives")
    void neverReturnsFalseNegative() {
        var filter = BloomFilter.create(10_000, 0.01);
        for (var i = 0; i < 10_000; i++) {
            filter.add(key("key-" + i));
        }
        for (var i = 0; i < 10_000; i++) {
            assertThat(filter.mightContain(key("key-" + i)))
                    .as("key-%d was added; a false negative here would lose durably written data", i)
                    .isTrue();
        }
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(ints = {1, 2, 3, 17, 99})
    @DisplayName("no false negatives for random binary keys either")
    void neverReturnsFalseNegativeForRandomKeys(int seed) {
        var random = new Random(seed);
        var filter = BloomFilter.create(5_000, 0.01);
        var keys = new byte[5_000][];

        for (var i = 0; i < keys.length; i++) {
            keys[i] = new byte[1 + random.nextInt(40)];
            random.nextBytes(keys[i]); // includes high/negative bytes
            filter.add(keys[i]);
        }
        for (var k : keys) {
            assertThat(filter.mightContain(k)).isTrue();
        }
    }

    @Test
    @DisplayName("the false positive rate is near the configured target")
    void falsePositiveRateIsReasonable() {
        var filter = BloomFilter.create(10_000, 0.01);
        for (var i = 0; i < 10_000; i++) {
            filter.add(key("present-" + i));
        }

        var falsePositives = 0;
        var trials = 100_000;
        for (var i = 0; i < trials; i++) {
            if (filter.mightContain(key("absent-" + i))) {
                falsePositives++;
            }
        }
        var rate = (double) falsePositives / trials;

        // Generous bound: the target is 1%, and the point is that the filter is actually filtering
        // rather than saying "maybe" to everything, not that it hits the theoretical optimum.
        assertThat(rate).as("observed false positive rate %.4f".formatted(rate)).isLessThan(0.05);
    }

    @Test
    @DisplayName("an empty filter rejects everything")
    void emptyFilterRejects() {
        var filter = BloomFilter.create(1000, 0.01);
        assertThat(filter.mightContain(key("anything"))).isFalse();
    }

    @Test
    @DisplayName("a filter survives being written to disk and read back")
    void roundTripsThroughSerialization() throws Exception {
        var original = BloomFilter.create(1000, 0.01);
        for (var i = 0; i < 1000; i++) {
            original.add(key("key-" + i));
        }

        var buffer = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(buffer)) {
            original.writeTo(out);
        }
        var restored = BloomFilter.readFrom(
                new DataInputStream(new ByteArrayInputStream(buffer.toByteArray())));

        assertThat(restored.bitCount()).isEqualTo(original.bitCount());
        assertThat(restored.hashCount()).isEqualTo(original.hashCount());
        // The critical property has to survive the round trip: a filter that lost bits on reload
        // would start producing false negatives against data already on disk.
        for (var i = 0; i < 1000; i++) {
            assertThat(restored.mightContain(key("key-" + i))).isTrue();
        }
    }

    @Test
    @DisplayName("invalid sizing parameters are rejected rather than silently accepted")
    void rejectsInvalidParameters() {
        assertThatThrownBy(() -> BloomFilter.create(0, 0.01)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BloomFilter.create(100, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BloomFilter.create(100, 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("empty and single-byte keys are handled without error")
    void handlesEdgeCaseKeys() {
        var filter = BloomFilter.create(100, 0.01);
        var empty = new byte[0];
        var single = new byte[] {(byte) 0xFF};
        filter.add(empty);
        filter.add(single);

        assertThat(filter.mightContain(empty)).isTrue();
        assertThat(filter.mightContain(single)).isTrue();
    }
}
