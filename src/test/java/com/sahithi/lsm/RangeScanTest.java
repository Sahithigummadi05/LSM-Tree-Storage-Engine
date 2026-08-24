package com.sahithi.lsm;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Random;
import java.util.TreeMap;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Range scans, checked against a {@link TreeMap} that obviously behaves correctly.
 *
 * <p>Scans are where the merge logic can go quietly wrong in a way point lookups never reveal. A
 * {@code get()} consults sources newest-first and returns the first hit, so recency is handled by
 * the loop itself. A scan has to merge every source simultaneously and decide, key by key, which
 * version wins — get that wrong and lookups stay perfectly correct while scans hand back stale
 * values or resurrect deleted keys.
 */
class RangeScanTest {

    @TempDir
    Path tempDir;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("a scan returns keys in sorted order")
    void scanReturnsSortedOrder() throws Exception {
        try (var store = new LsmStore(tempDir)) {
            store.put("delta", "4");
            store.put("alpha", "1");
            store.put("charlie", "3");
            store.put("bravo", "2");

            assertThat(store.scanKeys(null, null))
                    .containsExactly("alpha", "bravo", "charlie", "delta");
        }
    }

    @Test
    @DisplayName("bounds are inclusive at the start and exclusive at the end")
    void respectsRangeBounds() throws Exception {
        try (var store = new LsmStore(tempDir)) {
            for (var c : "abcdef".toCharArray()) {
                store.put(String.valueOf(c), "v");
            }

            assertThat(store.scanKeys("b", "e")).containsExactly("b", "c", "d");
            assertThat(store.scanKeys("b", null)).containsExactly("b", "c", "d", "e", "f");
            assertThat(store.scanKeys(null, "c")).containsExactly("a", "b");
            assertThat(store.scanKeys("x", "z")).isEmpty();
        }
    }

    @Test
    @DisplayName("a scan merges the memtable with data already on disk")
    void mergesMemtableAndSSTables() throws Exception {
        try (var store = new LsmStore(tempDir)) {
            store.put("a", "1");
            store.put("c", "3");
            store.flush(); // a and c are now on disk
            store.put("b", "2");
            store.put("d", "4"); // b and d are still in memory

            assertThat(store.scanKeys(null, null)).containsExactly("a", "b", "c", "d");
        }
    }

    @Test
    @DisplayName("a scan returns the newest version of an overwritten key, exactly once")
    void newestVersionWinsInScan() throws Exception {
        try (var store = new LsmStore(tempDir, 512, 100, false)) {
            store.put("k", "old");
            store.flush();
            store.put("k", "middle");
            store.flush();
            store.put("k", "newest");

            var results = store.scan(null, null).toList();

            assertThat(results).hasSize(1); // not three copies of the same key
            assertThat(new String(results.get(0).value())).isEqualTo("newest");
        }
    }

    @Test
    @DisplayName("a deleted key never appears in a scan, even with older versions on disk")
    void deletedKeysAreExcluded() throws Exception {
        try (var store = new LsmStore(tempDir, 512, 100, false)) {
            store.put("keep", "v");
            store.put("remove", "v");
            store.flush();
            store.delete("remove");
            store.flush();

            // The tombstone must win the merge over the older value and then be dropped.
            assertThat(store.scanKeys(null, null)).containsExactly("keep");
        }
    }

    @Test
    @DisplayName("a key deleted and then rewritten reappears in a scan")
    void reinsertedKeyAppears() throws Exception {
        try (var store = new LsmStore(tempDir, 512, 100, false)) {
            store.put("k", "v1");
            store.flush();
            store.delete("k");
            store.flush();
            store.put("k", "v2");

            var results = store.scan(null, null).toList();
            assertThat(results).hasSize(1);
            assertThat(new String(results.get(0).value())).isEqualTo("v2");
        }
    }

    @Test
    @DisplayName("scans still work after compaction")
    void scanAfterCompaction() throws Exception {
        try (var store = new LsmStore(tempDir, 512, 100, false)) {
            for (var i = 0; i < 20; i++) {
                store.put("key-%02d".formatted(i), "v" + i);
                if (i % 5 == 0) {
                    store.flush();
                }
            }
            store.flush();
            store.compact();

            var keys = store.scanKeys(null, null);
            assertThat(keys).hasSize(20);
            assertThat(keys).isSorted();
        }
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(ints = {1, 7, 42, 123, 2024})
    @DisplayName("scans match a TreeMap reference model across randomized workloads")
    void matchesReferenceModel(int seed) throws Exception {
        var model = new TreeMap<String, String>();
        var random = new Random(seed);
        var dir = tempDir.resolve("seed-" + seed);

        try (var store = new LsmStore(dir, 1024, 4, false)) {
            for (var step = 0; step < 2000; step++) {
                var key = "key-%03d".formatted(random.nextInt(200));
                if (random.nextInt(100) < 25) {
                    store.delete(key);
                    model.remove(key);
                } else {
                    var value = "v" + random.nextInt(1000);
                    store.put(key, value);
                    model.put(key, value);
                }
            }

            // Full scan
            assertThat(store.scanKeys(null, null))
                    .as("full scan must match the model exactly")
                    .containsExactlyElementsOf(model.keySet());

            // Bounded scans over randomly chosen windows
            for (var trial = 0; trial < 20; trial++) {
                var lo = "key-%03d".formatted(random.nextInt(200));
                var hi = "key-%03d".formatted(random.nextInt(200));
                if (lo.compareTo(hi) > 0) {
                    var swap = lo;
                    lo = hi;
                    hi = swap;
                }
                assertThat(store.scanKeys(lo, hi))
                        .as("range [%s, %s)", lo, hi)
                        .containsExactlyElementsOf(model.subMap(lo, hi).keySet());
            }

            // Values must be right too, not just the key set.
            var scanner = store.scan(null, null);
            while (scanner.hasNext()) {
                var record = scanner.next();
                var key = new String(record.key(), StandardCharsets.UTF_8);
                assertThat(new String(record.value(), StandardCharsets.UTF_8))
                        .as("value for %s", key)
                        .isEqualTo(model.get(key));
            }
        }
    }

    @Test
    @DisplayName("scanning an empty store yields nothing rather than failing")
    void emptyStoreScansCleanly() throws Exception {
        try (var store = new LsmStore(tempDir)) {
            assertThat(store.scan(null, null).toList()).isEmpty();
            assertThat(store.scan(bytes("a"), bytes("z")).toList()).isEmpty();
        }
    }
}
