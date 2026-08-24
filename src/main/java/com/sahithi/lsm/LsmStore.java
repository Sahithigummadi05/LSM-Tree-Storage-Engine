package com.sahithi.lsm;

import com.sahithi.lsm.memtable.Memtable;
import com.sahithi.lsm.sstable.SSTable;
import com.sahithi.lsm.wal.WriteAheadLog;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * A log-structured merge-tree key-value store.
 *
 * <h2>Why this shape</h2>
 *
 * <p>A B-tree updates data in place: to change a key you find its page and rewrite it, which means
 * a random seek per write. An LSM tree never overwrites anything. Writes go to an in-memory
 * memtable plus a sequential log append; when the memtable fills it is flushed whole into a new
 * immutable sorted file. Every write is therefore sequential I/O, which is dramatically faster
 * than random I/O on both spinning disks and SSDs.
 *
 * <p>Nothing is free — the cost moves to reads. A key may live in the memtable or in any SSTable,
 * so a lookup checks newest to oldest and stops at the first hit. Bloom filters keep most of those
 * checks from touching disk. And because old versions of a key pile up across files, background
 * <b>compaction</b> merges tables and discards shadowed records, trading write bandwidth for read
 * speed and disk space. That three-way tension between write throughput, read latency, and space
 * is the entire design space of this kind of engine.
 *
 * <h2>Read path</h2>
 *
 * <p>Newest to oldest, first hit wins: memtable, then SSTables in reverse age order. Ordering is
 * not an optimisation here, it is correctness — an older table legitimately holds a previous value
 * for the same key, so consulting it first would return stale data. A tombstone found along the
 * way terminates the search and reports absence, which is what stops a delete from being undone by
 * an older table further down.
 */
public final class LsmStore implements AutoCloseable {

    private static final String WAL_FILE = "wal.log";
    private static final String SSTABLE_PREFIX = "sstable-";
    private static final String SSTABLE_SUFFIX = ".db";

    private final Path directory;
    private final long memtableFlushBytes;
    private final int compactionTriggerFileCount;
    private final boolean fsyncOnWrite;

    private Memtable memtable = new Memtable();
    private WriteAheadLog wal;
    /** Oldest first, so the read path iterates it in reverse. */
    private final List<SSTable> ssTables = new ArrayList<>();
    private final AtomicLong nextTableId = new AtomicLong();

    private long flushCount;
    private long compactionCount;
    private long bytesWrittenToSSTables;

    public LsmStore(Path directory) throws IOException {
        this(directory, 1 << 20, 4, true);
    }

    /**
     * @param memtableFlushBytes flush the memtable to an SSTable once it exceeds this size
     * @param compactionTriggerFileCount compact once this many SSTables exist
     * @param fsyncOnWrite force each write to physical storage before acknowledging it. Disabling
     *     it makes writes much faster and makes the store lose recent writes on power loss — the
     *     central durability/throughput trade, exposed rather than hidden.
     */
    public LsmStore(Path directory, long memtableFlushBytes, int compactionTriggerFileCount,
            boolean fsyncOnWrite) throws IOException {
        this.directory = directory;
        this.memtableFlushBytes = memtableFlushBytes;
        this.compactionTriggerFileCount = compactionTriggerFileCount;
        this.fsyncOnWrite = fsyncOnWrite;
        Files.createDirectories(directory);
        recover();
    }

    // ------------------------------------------------------------------
    // Recovery
    // ------------------------------------------------------------------

    /**
     * Rebuilds state after a restart, clean or otherwise.
     *
     * <p>Existing SSTables are loaded oldest-first. Any that fail to open lacked a valid footer,
     * meaning they were being written when the process died — they are deleted rather than
     * tolerated, and the records they would have held are still in the log. Replaying the log then
     * restores exactly the memtable that was lost.
     */
    private void recover() throws IOException {
        try (Stream<Path> files = Files.list(directory)) {
            var tables = files
                    .filter(p -> p.getFileName().toString().startsWith(SSTABLE_PREFIX))
                    .filter(p -> p.getFileName().toString().endsWith(SSTABLE_SUFFIX))
                    .sorted(Comparator.comparingLong(LsmStore::tableIdOf))
                    .toList();

            for (var path : tables) {
                try {
                    ssTables.add(SSTable.open(path));
                    nextTableId.set(Math.max(nextTableId.get(), tableIdOf(path) + 1));
                } catch (IOException e) {
                    // Unfinished flush from a crash. Safe to discard: the write-ahead log still
                    // holds every record this file was going to contain.
                    Files.deleteIfExists(path);
                }
            }
        }

        // Clean up any temp files left behind by a crash during flush.
        try (Stream<Path> files = Files.list(directory)) {
            for (var path : files.filter(p -> p.getFileName().toString().endsWith(".tmp")).toList()) {
                Files.deleteIfExists(path);
            }
        }

        var walPath = directory.resolve(WAL_FILE);
        for (var record : WriteAheadLog.replay(walPath)) {
            memtable.put(record);
        }
        wal = new WriteAheadLog(walPath, fsyncOnWrite);
    }

    private static long tableIdOf(Path path) {
        var name = path.getFileName().toString();
        var digits = name.substring(SSTABLE_PREFIX.length(), name.length() - SSTABLE_SUFFIX.length());
        return Long.parseLong(digits);
    }

    // ------------------------------------------------------------------
    // Writes
    // ------------------------------------------------------------------

    public synchronized void put(byte[] key, byte[] value) throws IOException {
        write(Record.of(key, value));
    }

    public void put(String key, String value) throws IOException {
        put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    public synchronized void delete(byte[] key) throws IOException {
        write(Record.deletion(key));
    }

    public void delete(String key) throws IOException {
        delete(key.getBytes(StandardCharsets.UTF_8));
    }

    private void write(Record record) throws IOException {
        // Log first, then apply. If the order were reversed, a crash between the two would leave a
        // write visible in memory that recovery could not restore.
        wal.append(record);
        memtable.put(record);

        if (memtable.approximateBytes() >= memtableFlushBytes) {
            flush();
        }
    }

    // ------------------------------------------------------------------
    // Reads
    // ------------------------------------------------------------------

    /** @return the stored value, or null if the key is absent or deleted. */
    public synchronized byte[] get(byte[] key) throws IOException {
        var fromMemtable = memtable.get(key);
        if (fromMemtable != null) {
            return fromMemtable.tombstone() ? null : fromMemtable.value();
        }

        // Newest table first. A hit here shadows anything older, including a tombstone that must
        // suppress a value still present further down.
        for (var i = ssTables.size() - 1; i >= 0; i--) {
            var record = ssTables.get(i).get(key);
            if (record != null) {
                return record.tombstone() ? null : record.value();
            }
        }
        return null;
    }

    public String getString(String key) throws IOException {
        var value = get(key.getBytes(StandardCharsets.UTF_8));
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    // ------------------------------------------------------------------
    // Flush and compaction
    // ------------------------------------------------------------------

    /** Writes the memtable out as a new immutable SSTable and clears the log. */
    public synchronized void flush() throws IOException {
        if (memtable.isEmpty()) {
            return;
        }
        var path = directory.resolve(SSTABLE_PREFIX + nextTableId.getAndIncrement() + SSTABLE_SUFFIX);
        SSTable.write(path, memtable.records());
        bytesWrittenToSSTables += Files.size(path);
        ssTables.add(SSTable.open(path));

        // Only safe to discard the log once the SSTable is durably on disk - which SSTable.write
        // guarantees by fsyncing before its atomic rename.
        memtable = new Memtable();
        wal.truncate();
        flushCount++;

        if (ssTables.size() >= compactionTriggerFileCount) {
            compact();
        }
    }

    /**
     * Merges every SSTable into one, keeping only the newest record per key.
     *
     * <p>This is a full merge for clarity rather than a levelled or size-tiered scheme; the
     * mechanism that matters — resolving duplicate keys by recency and reclaiming shadowed data —
     * is the same either way.
     *
     * <p><b>Tombstones are only dropped when every table is being merged.</b> A tombstone exists to
     * shadow older values, so discarding it while an unmerged older table still holds that key
     * would resurrect deleted data. Because this compaction always consumes all tables, nothing
     * older can survive to contradict it, and dropping tombstones is safe here — a partial
     * compaction would have to keep them.
     */
    public synchronized void compact() throws IOException {
        if (ssTables.size() < 2) {
            return;
        }

        var merged = new java.util.TreeMap<byte[], Record>(Memtable.KEY_ORDER);
        // Oldest to newest, so later puts overwrite earlier ones and the newest version wins.
        for (var table : ssTables) {
            for (var record : table.readAll()) {
                merged.put(record.key(), record);
            }
        }

        var survivors = merged.values().stream().filter(r -> !r.tombstone()).toList();

        var path = directory.resolve(SSTABLE_PREFIX + nextTableId.getAndIncrement() + SSTABLE_SUFFIX);
        SSTable.write(path, survivors);
        bytesWrittenToSSTables += Files.size(path);

        var replaced = List.copyOf(ssTables);
        ssTables.clear();
        ssTables.add(SSTable.open(path));

        // Delete only after the replacement is durable and installed, so a crash mid-compaction
        // leaves the old tables intact and recoverable.
        for (var table : replaced) {
            table.close();
            Files.deleteIfExists(table.path());
        }
        compactionCount++;
    }

    // ------------------------------------------------------------------
    // Observability
    // ------------------------------------------------------------------

    public synchronized int ssTableCount() {
        return ssTables.size();
    }

    public synchronized long flushCount() {
        return flushCount;
    }

    public synchronized long compactionCount() {
        return compactionCount;
    }

    /**
     * Total bytes written to SSTables. Compared against the logical bytes a caller supplied, this
     * is <b>write amplification</b> — the defining cost of an LSM tree, since compaction rewrites
     * data that was already on disk.
     */
    public synchronized long bytesWrittenToSSTables() {
        return bytesWrittenToSSTables;
    }

    public Path directory() {
        return directory;
    }

    @Override
    public synchronized void close() throws IOException {
        wal.close();
        for (var table : ssTables) {
            table.close();
        }
    }
}
