package com.sahithi.lsm;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The test the whole write-ahead log exists for: <b>a write that was acknowledged must survive the
 * process being killed.</b>
 *
 * <p>Almost every storage bug that matters hides here, and almost none of it is reachable by
 * calling {@code close()} politely at the end of a test. A store that never flushes still passes a
 * happy-path test, because the data is sitting in memory where {@code get()} can find it. The only
 * way to know durability works is to destroy the process without warning and then check.
 *
 * <p>So these tests spawn a <b>real child JVM</b> that writes records and then calls
 * {@link Runtime#halt(int)} — which terminates immediately, running no shutdown hooks, flushing no
 * buffers, closing no files. It is the closest thing to {@code kill -9} available from inside
 * Java, and much harsher than a normal exit. A fresh store is then opened over the same directory
 * to see what actually survived.
 */
class CrashDurabilityTest {

    @TempDir
    Path tempDir;

    /**
     * Runs {@link CrashWriter} in a separate JVM. It writes {@code count} records, prints how many
     * it acknowledged, and halts without cleanup.
     */
    private int runCrashingWriter(Path directory, int count, boolean fsync) throws Exception {
        var java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        var classpath = System.getProperty("java.class.path");

        var process = new ProcessBuilder(
                java, "-cp", classpath, CrashWriter.class.getName(),
                directory.toString(), String.valueOf(count), String.valueOf(fsync))
                .redirectErrorStream(true)
                .start();

        var output = new String(process.getInputStream().readAllBytes());
        assertThat(process.waitFor(120, TimeUnit.SECONDS)).as("crash writer timed out").isTrue();

        // The child halts with a nonzero status on purpose - that is the simulated crash.
        var marker = "ACKNOWLEDGED=";
        var index = output.indexOf(marker);
        assertThat(index).as("child JVM did not report progress. Output:%n%s", output).isNotNegative();
        var line = output.substring(index + marker.length()).lines().findFirst().orElseThrow();
        return Integer.parseInt(line.trim());
    }

    @Test
    @DisplayName("every acknowledged write survives a hard kill with no clean shutdown")
    void acknowledgedWritesSurviveCrash() throws Exception {
        var acknowledged = runCrashingWriter(tempDir, 500, true);
        assertThat(acknowledged).isEqualTo(500);

        // Nothing was closed, no shutdown hook ran. Recovery has only the files on disk to work
        // with - and must reconstruct every acknowledged write from them.
        try (var recovered = new LsmStore(tempDir)) {
            for (var i = 0; i < acknowledged; i++) {
                assertThat(recovered.getString("key-" + i))
                        .as("key-%d was acknowledged before the crash and must survive", i)
                        .isEqualTo("value-" + i);
            }
        }
    }

    @Test
    @DisplayName("recovery works when the crash happened after some data had already flushed")
    void survivesCrashWithMixOfFlushedAndUnflushedData() throws Exception {
        // A small flush threshold guarantees the child crosses several flush boundaries, so the
        // crash lands with some records in SSTables and others only in the log.
        var acknowledged = runCrashingWriter(tempDir, 2000, true);

        var sstables = Files.list(tempDir).filter(p -> p.toString().endsWith(".db")).count();
        assertThat(sstables).as("expected at least one flush before the crash").isPositive();

        try (var recovered = new LsmStore(tempDir)) {
            for (var i = 0; i < acknowledged; i++) {
                assertThat(recovered.getString("key-" + i)).isEqualTo("value-" + i);
            }
        }
    }

    @Test
    @DisplayName("a torn record at the tail of the log is discarded, not treated as data")
    void truncatedLogTailIsIgnored() throws Exception {
        try (var store = new LsmStore(tempDir)) {
            store.put("alpha", "1");
            store.put("beta", "2");
        }

        // Simulate a process killed mid-append: chop bytes off the end of the log so the last
        // record is incomplete. Recovery must keep the intact records and drop the fragment
        // rather than reading garbage as a key or a length.
        var wal = tempDir.resolve("wal.log");
        var bytes = Files.readAllBytes(wal);
        Files.write(wal, java.util.Arrays.copyOf(bytes, bytes.length - 5));

        try (var recovered = new LsmStore(tempDir)) {
            assertThat(recovered.getString("alpha")).isEqualTo("1");
            // "beta" may or may not have survived depending on where the cut landed; what must not
            // happen is a crash or a corrupted read.
            var beta = recovered.getString("beta");
            assertThat(beta == null || beta.equals("2")).isTrue();
        }
    }

    @Test
    @DisplayName("a corrupt checksum stops recovery instead of loading bad data")
    void corruptRecordIsNotLoaded() throws Exception {
        try (var store = new LsmStore(tempDir)) {
            store.put("good", "value");
            store.put("corrupted", "value");
        }

        // Flip a byte inside the last record's payload. Its CRC no longer matches, so recovery
        // must treat it as a torn write rather than trusting it.
        var wal = tempDir.resolve("wal.log");
        var bytes = Files.readAllBytes(wal);
        bytes[bytes.length - 3] ^= 0xFF;
        Files.write(wal, bytes);

        try (var recovered = new LsmStore(tempDir)) {
            assertThat(recovered.getString("good")).isEqualTo("value");
            assertThat(recovered.getString("corrupted")).isNull();
        }
    }

    @Test
    @DisplayName("an SSTable left unfinished by a crash is discarded and its data recovered from the log")
    void unfinishedSSTableIsDiscarded() throws Exception {
        try (var store = new LsmStore(tempDir)) {
            store.put("k1", "v1");
            store.put("k2", "v2");
        }

        // A file that looks like an SSTable but has no valid footer - exactly what a crash during
        // flush leaves behind. It must be deleted on recovery, not opened.
        var bogus = tempDir.resolve("sstable-9999.db");
        Files.write(bogus, "not a real sstable".getBytes());

        try (var recovered = new LsmStore(tempDir)) {
            assertThat(recovered.getString("k1")).isEqualTo("v1");
            assertThat(recovered.getString("k2")).isEqualTo("v2");
        }
        assertThat(Files.exists(bogus)).as("unfinished SSTable should have been removed").isFalse();
    }

    @Test
    @DisplayName("documents the limit of this harness: it tests process crashes, not power loss")
    void processCrashSurvivesEvenWithoutFsync() throws Exception {
        // Deliberately disable fsync, then crash the process anyway.
        var acknowledged = runCrashingWriter(tempDir, 300, false);

        try (var recovered = new LsmStore(tempDir)) {
            var survived = 0;
            for (var i = 0; i < acknowledged; i++) {
                if (recovered.getString("key-" + i) != null) {
                    survived++;
                }
            }
            // Everything survives, because killing a JVM does not clear the operating system's
            // page cache - the bytes had already left the process and the OS still holds them.
            assertThat(survived)
                    .as("a process crash alone does not lose page-cached writes")
                    .isEqualTo(acknowledged);
        }

        // The honest consequence: these tests prove recovery is correct against process death
        // (kill -9, OOM kill, JVM crash), which is the common failure. They do NOT prove the
        // fsync path, because the scenario fsync defends against - power loss or kernel panic,
        // where the page cache evaporates - cannot be simulated from inside a process. Verifying
        // that needs real hardware losing power, or a fault-injecting filesystem. Said plainly in
        // the README rather than left for a reader to assume this test covers more than it can.
    }

    /**
     * Child process: writes records, reports how many were acknowledged, then dies instantly.
     *
     * <p>{@code Runtime.halt} rather than {@code System.exit}: exit runs shutdown hooks and would
     * give the store a chance to clean up, which is exactly the behaviour this test must not rely
     * on.
     */
    public static final class CrashWriter {
        public static void main(String[] args) throws IOException {
            var directory = Path.of(args[0]);
            var count = Integer.parseInt(args[1]);
            var fsync = Boolean.parseBoolean(args[2]);

            var store = new LsmStore(directory, 8 * 1024, 1000, fsync);
            var written = 0;
            for (var i = 0; i < count; i++) {
                store.put("key-" + i, "value-" + i);
                written++;
            }
            System.out.println("ACKNOWLEDGED=" + written);
            System.out.flush();

            Runtime.getRuntime().halt(9); // no hooks, no flush, no close
        }
    }
}
