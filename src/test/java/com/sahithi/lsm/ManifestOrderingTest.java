package com.sahithi.lsm;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for a silent data-corruption bug: <b>stale values resurrecting after a
 * restart</b>.
 *
 * <p>Reads walk tables newest-to-oldest and take the first hit, so table order is correctness, not
 * bookkeeping. In memory that order was right — compaction splices the merged table into the slot
 * of the <em>oldest</em> table it replaced. On disk it was wrong, because the merged table is given
 * a fresh (highest) id while recovery sorted by id. Any compaction that merged something other than
 * the newest tables therefore produced a file order that was reversed relative to reality.
 *
 * <p>What made it dangerous is that every in-process assertion passed. The store returned the
 * correct value before compaction, after compaction, and under any amount of testing that never
 * closed and reopened it. Only a restart revealed it, and then it returned an old value with no
 * error of any kind.
 *
 * <p>The fix is a manifest recording the true order, which is why real engines keep one.
 */
class ManifestOrderingTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("a value is not reverted by a restart after partial compaction")
    void staleValueDoesNotResurrectAfterReopen() throws Exception {
        try (var store = new LsmStore(tempDir, 1L << 30, 100_000, false)) {
            // Two small tables holding the OLD value.
            store.put("x", "OLD");
            store.flush();
            store.put("filler", "f");
            store.flush();

            // One much larger table holding the NEW value. Size-tiering will exclude it from the
            // merge, so the merged table lands *older* than it - the exact shape that broke.
            store.put("x", "NEW");
            for (var i = 0; i < 2000; i++) {
                store.put("pad-" + i, "padding-padding-padding-padding");
            }
            store.flush();

            store.compactSizeTiered();
            assertThat(store.getString("x")).as("in-memory order was always correct").isEqualTo("NEW");
        }

        try (var reopened = new LsmStore(tempDir)) {
            assertThat(reopened.getString("x"))
                    .as("restart must not resurrect the pre-compaction value")
                    .isEqualTo("NEW");
        }
    }

    @Test
    @DisplayName("a deletion is not undone by a restart after partial compaction")
    void deletionDoesNotResurrectAfterReopen() throws Exception {
        try (var store = new LsmStore(tempDir, 1L << 30, 100_000, false)) {
            store.put("doomed", "value");
            store.flush();
            store.put("other", "v");
            store.flush();

            store.delete("doomed");
            for (var i = 0; i < 2000; i++) {
                store.put("pad-" + i, "padding-padding-padding-padding");
            }
            store.flush();

            store.compactSizeTiered();
            assertThat(store.getString("doomed")).isNull();
        }

        try (var reopened = new LsmStore(tempDir)) {
            assertThat(reopened.getString("doomed"))
                    .as("a tombstone reordered behind its value would undo the delete")
                    .isNull();
        }
    }

    @Test
    @DisplayName("the manifest records table order and recovery follows it")
    void manifestIsWrittenAndUsed() throws Exception {
        try (var store = new LsmStore(tempDir, 512, 100_000, false)) {
            for (var i = 0; i < 5; i++) {
                store.put("key-" + i, "v" + i);
                store.flush();
            }
        }

        var manifest = tempDir.resolve("MANIFEST");
        assertThat(Files.exists(manifest)).isTrue();
        assertThat(Files.readAllLines(manifest)).hasSize(5).allMatch(line -> line.endsWith(".db"));
    }

    @Test
    @DisplayName("an SSTable missing from the manifest is treated as crash debris")
    void unlistedTableIsDiscarded() throws Exception {
        try (var store = new LsmStore(tempDir, 1L << 30, 100_000, false)) {
            store.put("real", "value");
            store.flush();
        }

        // A complete, readable SSTable that the manifest does not mention: what a crash between
        // writing a table and committing the manifest leaves behind. It must not be adopted, since
        // its contents were never part of the committed state.
        var listed = Files.readAllLines(tempDir.resolve("MANIFEST"));
        var orphan = tempDir.resolve("sstable-9999.db");
        Files.copy(tempDir.resolve(listed.get(0)), orphan);

        try (var reopened = new LsmStore(tempDir)) {
            assertThat(reopened.getString("real")).isEqualTo("value");
        }
        assertThat(Files.exists(orphan)).as("unlisted table should have been removed").isFalse();
    }

    @Test
    @DisplayName("values survive many rounds of compaction and reopening")
    void ordersStayCorrectAcrossRepeatedCompactionAndRestart() throws Exception {
        var model = new java.util.HashMap<String, String>();
        var random = new Random(31);

        for (var session = 0; session < 6; session++) {
            try (var store = new LsmStore(tempDir, 2048, 100_000, false)) {
                // Every value written this session must beat everything written before it.
                for (var i = 0; i < 300; i++) {
                    var key = "key-%03d".formatted(random.nextInt(120));
                    if (random.nextInt(100) < 20) {
                        store.delete(key);
                        model.remove(key);
                    } else {
                        var value = "session-" + session + "-" + i;
                        store.put(key, value);
                        model.put(key, value);
                    }
                }
                store.flush();
                store.compactSizeTiered();

                for (var entry : model.entrySet()) {
                    assertThat(store.getString(entry.getKey()))
                            .as("session %d, key %s", session, entry.getKey())
                            .isEqualTo(entry.getValue());
                }
            }

            // Reopen and re-check: this is where the ordering bug used to appear.
            try (var reopened = new LsmStore(tempDir)) {
                for (var entry : model.entrySet()) {
                    assertThat(reopened.getString(entry.getKey()))
                            .as("after reopen following session %d, key %s", session, entry.getKey())
                            .isEqualTo(entry.getValue());
                }
            }
        }
    }
}
