package com.sahithi.lsm;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Measures the three quantities that define an LSM tree's behaviour, so the design trade-offs are
 * observed rather than asserted:
 *
 * <ul>
 *   <li><b>Write throughput</b>, with and without fsync — the durability/speed trade in numbers.
 *   <li><b>Read latency</b>, for keys that exist and keys that don't — the second showing what
 *       bloom filters actually buy.
 *   <li><b>Write amplification</b> — bytes physically written per logical byte stored, which is
 *       the price paid for fast writes and the reason compaction tuning matters.
 * </ul>
 *
 * <p>Absolute timings depend on the machine and are not asserted on; the relationships between
 * them are the point.
 */
class BenchmarkTest {

    @TempDir
    Path tempDir;

    private static final int RECORDS = 50_000;

    @Test
    @DisplayName("measure write throughput, read latency, and write amplification")
    void measure() throws Exception {
        System.out.println("\n================ LSM storage engine ================");

        var withoutFsync = measureWrites(tempDir.resolve("nofsync"), false);
        System.out.printf("Write throughput (fsync off) : %,10.0f ops/sec%n", withoutFsync);

        var withFsync = measureWrites(tempDir.resolve("fsync"), true);
        System.out.printf("Write throughput (fsync on)  : %,10.0f ops/sec%n", withFsync);
        System.out.printf("  -> durability costs %.1fx throughput%n", withoutFsync / withFsync);

        measureReadsAndAmplification();
        System.out.println("====================================================\n");

        assertThat(withoutFsync).isPositive();
        assertThat(withFsync).isPositive();
    }

    private double measureWrites(Path dir, boolean fsync) throws Exception {
        try (var store = new LsmStore(dir, 4 << 20, 8, fsync)) {
            // fsync is genuinely slow, so a smaller sample keeps the test quick without changing
            // what it shows.
            var count = fsync ? 5_000 : RECORDS;
            var start = System.nanoTime();
            for (var i = 0; i < count; i++) {
                store.put("key-" + i, "value-" + i + "-padding-padding");
            }
            var elapsed = System.nanoTime() - start;
            return count / (elapsed / 1_000_000_000.0);
        }
    }

    private void measureReadsAndAmplification() throws Exception {
        var dir = tempDir.resolve("reads");
        try (var store = new LsmStore(dir, 512 * 1024, 6, false)) {
            var logicalBytes = 0L;
            for (var i = 0; i < RECORDS; i++) {
                var key = "key-" + i;
                var value = "value-" + i + "-padding-padding-padding";
                store.put(key, value);
                logicalBytes += key.length() + value.length();
            }
            store.flush();

            var random = new Random(42);

            // Keys that exist: the bloom filter says "maybe" and the file really is read.
            var start = System.nanoTime();
            var found = 0;
            for (var i = 0; i < 10_000; i++) {
                if (store.getString("key-" + random.nextInt(RECORDS)) != null) {
                    found++;
                }
            }
            var hitNanos = (System.nanoTime() - start) / 10_000;
            assertThat(found).isEqualTo(10_000);

            // Keys that don't exist: bloom filters should let most lookups skip the data entirely,
            // which is exactly what they are there for.
            start = System.nanoTime();
            for (var i = 0; i < 10_000; i++) {
                assertThat(store.getString("absent-" + random.nextInt(1_000_000))).isNull();
            }
            var missNanos = (System.nanoTime() - start) / 10_000;

            System.out.printf("Read latency (key present)   : %,10d ns%n", hitNanos);
            System.out.printf("Read latency (key absent)    : %,10d ns   <- bloom filter path%n", missNanos);

            var physicalBytes = store.bytesWrittenToSSTables();
            System.out.printf("Write amplification          : %10.2fx  (%,d logical -> %,d physical bytes)%n",
                    (double) physicalBytes / logicalBytes, logicalBytes, physicalBytes);
            System.out.printf("SSTables on disk             : %10d  (%d flushes, %d compactions)%n",
                    store.ssTableCount(), store.flushCount(), store.compactionCount());
        }
    }
}
