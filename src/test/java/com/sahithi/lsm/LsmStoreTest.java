package com.sahithi.lsm;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LsmStoreTest {

    @TempDir
    Path tempDir;

    @Nested
    @DisplayName("basic key-value semantics")
    class Basics {

        @Test
        @DisplayName("a value written can be read back")
        void readYourWrites() throws Exception {
            try (var store = new LsmStore(tempDir)) {
                store.put("name", "sahithi");
                assertThat(store.getString("name")).isEqualTo("sahithi");
            }
        }

        @Test
        @DisplayName("a missing key returns null")
        void missingKeyIsNull() throws Exception {
            try (var store = new LsmStore(tempDir)) {
                assertThat(store.getString("nope")).isNull();
            }
        }

        @Test
        @DisplayName("writing a key again returns the newer value")
        void overwriteWins() throws Exception {
            try (var store = new LsmStore(tempDir)) {
                store.put("k", "first");
                store.put("k", "second");
                assertThat(store.getString("k")).isEqualTo("second");
            }
        }

        @Test
        @DisplayName("a deleted key reads as absent")
        void deleteHidesValue() throws Exception {
            try (var store = new LsmStore(tempDir)) {
                store.put("k", "v");
                store.delete("k");
                assertThat(store.getString("k")).isNull();
            }
        }

        @Test
        @DisplayName("a key can be rewritten after being deleted")
        void writeAfterDelete() throws Exception {
            try (var store = new LsmStore(tempDir)) {
                store.put("k", "v1");
                store.delete("k");
                store.put("k", "v2");
                assertThat(store.getString("k")).isEqualTo("v2");
            }
        }

        @Test
        @DisplayName("binary keys with high bytes sort and retrieve correctly")
        void handlesUnsignedByteKeys() throws Exception {
            // Java bytes are signed, so 0x80 is negative. A naive comparator would sort these
            // wrongly and break the binary search over the SSTable index.
            try (var store = new LsmStore(tempDir, 128, 100, false)) {
                var low = new byte[] {0x01};
                var high = new byte[] {(byte) 0x80};
                var highest = new byte[] {(byte) 0xFF};
                store.put(low, "low".getBytes());
                store.put(high, "high".getBytes());
                store.put(highest, "highest".getBytes());
                store.flush();

                assertThat(new String(store.get(low))).isEqualTo("low");
                assertThat(new String(store.get(high))).isEqualTo("high");
                assertThat(new String(store.get(highest))).isEqualTo("highest");
            }
        }
    }

    @Nested
    @DisplayName("data crossing the memtable/disk boundary")
    class AcrossFlush {

        @Test
        @DisplayName("values remain readable after being flushed to an SSTable")
        void readAfterFlush() throws Exception {
            try (var store = new LsmStore(tempDir)) {
                for (var i = 0; i < 200; i++) {
                    store.put("key-" + i, "value-" + i);
                }
                store.flush();
                assertThat(store.ssTableCount()).isPositive();

                for (var i = 0; i < 200; i++) {
                    assertThat(store.getString("key-" + i)).isEqualTo("value-" + i);
                }
            }
        }

        @Test
        @DisplayName("a newer value in the memtable shadows an older one on disk")
        void memtableShadowsSSTable() throws Exception {
            try (var store = new LsmStore(tempDir)) {
                store.put("k", "old");
                store.flush();
                store.put("k", "new");

                assertThat(store.getString("k")).isEqualTo("new");
                store.flush();
                assertThat(store.getString("k")).isEqualTo("new");
            }
        }

        @Test
        @DisplayName("a delete shadows a value that is already on disk")
        void tombstoneShadowsSSTable() throws Exception {
            try (var store = new LsmStore(tempDir)) {
                store.put("k", "v");
                store.flush();
                store.delete("k");
                store.flush();

                // Two tables now exist: an older one holding the value and a newer one holding the
                // tombstone. Reading oldest-first would wrongly resurrect the value.
                assertThat(store.getString("k")).isNull();
            }
        }

        @Test
        @DisplayName("data survives closing and reopening the store")
        void survivesReopen() throws Exception {
            try (var store = new LsmStore(tempDir)) {
                store.put("persisted", "yes");
                store.put("deleted", "temporarily");
                store.delete("deleted");
            }
            try (var reopened = new LsmStore(tempDir)) {
                assertThat(reopened.getString("persisted")).isEqualTo("yes");
                assertThat(reopened.getString("deleted")).isNull();
            }
        }
    }

    @Nested
    @DisplayName("compaction")
    class Compaction {

        @Test
        @DisplayName("compaction merges many tables into one")
        void compactionReducesFileCount() throws Exception {
            try (var store = new LsmStore(tempDir, 512, 100, false)) {
                for (var i = 0; i < 8; i++) {
                    store.put("key-" + i, "value-" + i);
                    store.flush();
                }
                assertThat(store.ssTableCount()).isEqualTo(8);

                store.compact();

                assertThat(store.ssTableCount()).isEqualTo(1);
                for (var i = 0; i < 8; i++) {
                    assertThat(store.getString("key-" + i)).isEqualTo("value-" + i);
                }
            }
        }

        @Test
        @DisplayName("a deleted key stays deleted after compaction")
        void compactionDoesNotResurrectDeletedKeys() throws Exception {
            // The classic LSM bug: compaction drops the tombstone but keeps the older value from
            // another table, so a deleted key reappears. Anyone who has run a production LSM store
            // has seen a variant of this.
            try (var store = new LsmStore(tempDir, 512, 100, false)) {
                store.put("ghost", "should-not-come-back");
                store.flush();
                store.delete("ghost");
                store.flush();
                assertThat(store.ssTableCount()).isEqualTo(2);

                store.compact();

                assertThat(store.getString("ghost"))
                        .as("compaction must not resurrect a deleted key")
                        .isNull();
            }
        }

        @Test
        @DisplayName("compaction keeps the newest value when a key was written repeatedly")
        void compactionKeepsNewestVersion() throws Exception {
            try (var store = new LsmStore(tempDir, 512, 100, false)) {
                for (var version = 1; version <= 5; version++) {
                    store.put("k", "v" + version);
                    store.flush();
                }
                store.compact();

                assertThat(store.getString("k")).isEqualTo("v5");
                assertThat(store.ssTableCount()).isEqualTo(1);
            }
        }

        @Test
        @DisplayName("compaction reclaims space taken by shadowed versions")
        void compactionReclaimsSpace() throws Exception {
            try (var store = new LsmStore(tempDir, 4096, 1000, false)) {
                // Same 50 keys written 10 times over: 500 records, only 50 of them still relevant.
                for (var round = 0; round < 10; round++) {
                    for (var i = 0; i < 50; i++) {
                        store.put("key-" + i, "round-" + round + "-padding-padding-padding");
                    }
                    store.flush();
                }
                var before = totalSSTableBytes();

                store.compact();
                var after = totalSSTableBytes();

                assertThat(after).as("compaction should discard shadowed versions").isLessThan(before);
                for (var i = 0; i < 50; i++) {
                    assertThat(store.getString("key-" + i)).startsWith("round-9-");
                }
            }
        }

        private long totalSSTableBytes() throws Exception {
            try (var files = Files.list(tempDir)) {
                return files.filter(p -> p.toString().endsWith(".db"))
                        .mapToLong(p -> {
                            try {
                                return Files.size(p);
                            } catch (Exception e) {
                                return 0;
                            }
                        })
                        .sum();
            }
        }
    }

    @Nested
    @DisplayName("behaviour under a randomized workload")
    class Randomized {

        @Test
        @DisplayName("the store agrees with an in-memory model across thousands of mixed operations")
        void matchesReferenceModel() throws Exception {
            // Differential testing: run the same operations against a plain HashMap whose behaviour
            // is obviously correct, and require the engine to agree at every step. This is what
            // catches interactions between flush, compaction, tombstones and recovery that no
            // hand-written case would think to combine.
            var model = new java.util.HashMap<String, String>();
            var random = new Random(42);
            var keys = new ArrayList<String>();
            for (var i = 0; i < 200; i++) {
                keys.add("key-" + i);
            }

            try (var store = new LsmStore(tempDir, 2048, 4, false)) {
                for (var step = 0; step < 5000; step++) {
                    var key = keys.get(random.nextInt(keys.size()));
                    if (random.nextInt(100) < 25) {
                        store.delete(key);
                        model.remove(key);
                    } else {
                        var value = "v" + random.nextInt(1000);
                        store.put(key, value);
                        model.put(key, value);
                    }

                    if (step % 500 == 0) {
                        for (var k : keys) {
                            assertThat(store.getString(k))
                                    .as("mismatch on %s at step %d", k, step)
                                    .isEqualTo(model.get(k));
                        }
                    }
                }

                for (var k : keys) {
                    assertThat(store.getString(k)).as("final mismatch on %s", k).isEqualTo(model.get(k));
                }
                assertThat(store.compactionCount()).as("workload should have triggered compaction").isPositive();
            }
        }

        @Test
        @DisplayName("the model still matches after closing and reopening")
        void modelMatchesAfterReopen() throws Exception {
            var model = new java.util.HashMap<String, String>();
            var random = new Random(7);

            try (var store = new LsmStore(tempDir, 1024, 3, false)) {
                for (var i = 0; i < 1000; i++) {
                    var key = "key-" + random.nextInt(150);
                    if (random.nextInt(100) < 30) {
                        store.delete(key);
                        model.remove(key);
                    } else {
                        var value = "value-" + i;
                        store.put(key, value);
                        model.put(key, value);
                    }
                }
            }

            try (var reopened = new LsmStore(tempDir)) {
                for (var i = 0; i < 150; i++) {
                    var key = "key-" + i;
                    assertThat(reopened.getString(key)).as("after reopen: %s", key).isEqualTo(model.get(key));
                }
            }
        }
    }
}
