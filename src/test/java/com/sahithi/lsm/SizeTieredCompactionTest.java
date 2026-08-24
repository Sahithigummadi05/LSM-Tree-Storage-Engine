package com.sahithi.lsm;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Compares the two compaction strategies on the metric that distinguishes them: write
 * amplification, the bytes physically written per logical byte stored.
 *
 * <p>Full compaction rewrites every table on every run, so its cost scales with the whole dataset
 * regardless of how little changed. Size-tiering merges only tables of comparable size, so a large
 * table is rewritten rarely and work stays proportional to what is actually being reorganised.
 *
 * <p>The trade is not free — more tables can hold a given key, so reads may consult more of them.
 * These tests measure the write side and assert correctness is preserved on both paths.
 */
class SizeTieredCompactionTest {

    @TempDir
    Path tempDir;

    /**
     * Writes a workload where the dataset <em>grows</em> — mostly new keys, with a minority of
     * overwrites — and reports the bytes physically written.
     *
     * <p>A growing dataset is the case that separates the two strategies. On a fixed key space
     * both cost the same per round and the ratio between them is a constant, which is exactly what
     * an earlier version of this test measured and (correctly) refused to call a scaling win.
     */
    private long runWorkload(Path dir, boolean sizeTiered, int rounds) throws Exception {
        try (var store = new LsmStore(dir, 8 * 1024, 1000, false)) {
            var random = new Random(99);
            var nextKey = 0;
            for (var round = 0; round < rounds; round++) {
                for (var i = 0; i < 200; i++) {
                    String key;
                    if (nextKey > 0 && random.nextInt(100) < 20) {
                        key = "key-%06d".formatted(random.nextInt(nextKey)); // overwrite
                    } else {
                        key = "key-%06d".formatted(nextKey++); // new key: dataset grows
                    }
                    store.put(key, "value-" + round + "-padding-padding-padding");
                }
                store.flush();
                if (sizeTiered) {
                    store.compactSizeTiered();
                } else {
                    store.compact();
                }
            }
            return store.bytesWrittenToSSTables();
        }
    }

    @Test
    @DisplayName("size-tiering's advantage grows with the dataset, which is the actual claim")
    void sizeTieredReducesWriteAmplification() throws Exception {
        System.out.printf("%n=== Compaction strategy: bytes written ===%n");
        System.out.printf("%-8s %-16s %-16s %-10s%n", "rounds", "full", "size-tiered", "saved");

        var reductions = new java.util.ArrayList<Double>();
        for (var rounds : new int[] {10, 20, 40, 80}) {
            var full = runWorkload(tempDir.resolve("full-" + rounds), false, rounds);
            var tiered = runWorkload(tempDir.resolve("tiered-" + rounds), true, rounds);
            var reduction = 100.0 * (full - tiered) / full;
            reductions.add(reduction);

            System.out.printf("%-8d %,-16d %,-16d %8.1f%%%n", rounds, full, tiered, reduction);
            assertThat(tiered)
                    .as("size-tiering should write less at %d rounds", rounds)
                    .isLessThan(full);
        }
        System.out.println();

        // The point isn't a fixed percentage - it's that full compaction rewrites the whole
        // dataset every time, so the gap widens as data grows.
        assertThat(reductions.get(reductions.size() - 1))
                .as("the saving should be larger on the bigger dataset than the smallest one")
                .isGreaterThan(reductions.get(0));
    }

    @Test
    @DisplayName("size-tiered compaction preserves the newest value for every key")
    void sizeTieredKeepsCorrectValues() throws Exception {
        var model = new java.util.HashMap<String, String>();
        try (var store = new LsmStore(tempDir, 4096, 1000, false)) {
            var random = new Random(5);
            for (var round = 0; round < 30; round++) {
                for (var i = 0; i < 100; i++) {
                    var key = "key-%03d".formatted(random.nextInt(200));
                    var value = "round-" + round + "-item-" + i;
                    store.put(key, value);
                    model.put(key, value);
                }
                store.flush();
                store.compactSizeTiered();
            }

            for (var entry : model.entrySet()) {
                assertThat(store.getString(entry.getKey()))
                        .as("key %s after size-tiered compaction", entry.getKey())
                        .isEqualTo(entry.getValue());
            }
        }
    }

    @Test
    @DisplayName("size-tiered compaction keeps tombstones so deletes are not undone")
    void sizeTieredPreservesTombstones() throws Exception {
        try (var store = new LsmStore(tempDir, 512, 10000, false)) {
            // A large old table holding a value.
            for (var i = 0; i < 300; i++) {
                store.put("key-%03d".formatted(i), "value-with-padding-to-grow-the-file");
            }
            store.flush();

            // Then a small, recent table holding the tombstone.
            store.delete("key-100");
            store.flush();

            // Merging only the small tables must NOT drop that tombstone: the big table outside
            // this merge still holds the old value, and dropping it would resurrect the key.
            store.compactSizeTiered();

            assertThat(store.getString("key-100"))
                    .as("a tombstone dropped during a partial merge would resurrect this key")
                    .isNull();
            assertThat(store.getString("key-101")).isNotNull();
        }
    }

    @Test
    @DisplayName("size-tiered compaction leaves scans correct")
    void scansStayCorrect() throws Exception {
        var model = new java.util.TreeMap<String, String>();
        try (var store = new LsmStore(tempDir, 2048, 1000, false)) {
            var random = new Random(11);
            for (var round = 0; round < 20; round++) {
                for (var i = 0; i < 100; i++) {
                    var key = "key-%03d".formatted(random.nextInt(150));
                    if (random.nextInt(100) < 20) {
                        store.delete(key);
                        model.remove(key);
                    } else {
                        var value = "v" + round + "-" + i;
                        store.put(key, value);
                        model.put(key, value);
                    }
                }
                store.flush();
                store.compactSizeTiered();
            }

            assertThat(store.scanKeys(null, null)).containsExactlyElementsOf(model.keySet());
        }
    }

    @Test
    @DisplayName("compaction is a no-op when there is nothing worth merging")
    void noOpWhenSingleTable() throws Exception {
        try (var store = new LsmStore(tempDir, 1 << 20, 1000, false)) {
            store.put("k", "v");
            store.flush();
            var before = store.compactionCount();

            store.compactSizeTiered();

            assertThat(store.compactionCount()).isEqualTo(before);
            assertThat(store.getString("k")).isEqualTo("v");
        }
    }
}
