package com.sahithi.lsm.memtable;

import com.sahithi.lsm.Record;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The in-memory write buffer that absorbs every write before it reaches disk.
 *
 * <p>This is what makes LSM writes fast: a write is an in-memory insert plus a sequential log
 * append, never a random disk seek to update a page in place. The cost is deferred — the data has
 * to be organised and merged later, during flush and compaction.
 *
 * <p>Kept <b>sorted</b> by key, which matters for two reasons: flushing produces a sorted SSTable
 * in one linear pass with no sort step, and sorted files can later be merged with a simple
 * k-way merge. A hash map would be marginally faster to write but would force a full sort on
 * every flush and make merging much more expensive.
 *
 * <p>{@link ConcurrentSkipListMap} rather than a {@code TreeMap} with a lock: reads are lock-free
 * and concurrent with writes, which matters because a get() consults the memtable on every lookup.
 */
public final class Memtable {

    /**
     * Byte-wise unsigned comparison. Java's {@code byte} is signed, so a naive comparison would
     * order 0x80 before 0x00 and produce files that are not sorted the way readers expect —
     * silently breaking binary search over the SSTable index.
     */
    public static final Comparator<byte[]> KEY_ORDER = Arrays::compareUnsigned;

    private final ConcurrentSkipListMap<byte[], Record> entries = new ConcurrentSkipListMap<>(KEY_ORDER);
    private final AtomicLong approximateBytes = new AtomicLong();

    public void put(Record record) {
        var previous = entries.put(record.key(), record);
        approximateBytes.addAndGet(sizeOf(record) - (previous == null ? 0 : sizeOf(previous)));
    }

    /** @return the record for this key, or null if this memtable has never seen it. */
    public Record get(byte[] key) {
        return entries.get(key);
    }

    /** Records in key order - exactly the order an SSTable needs them written in. */
    public Collection<Record> records() {
        return entries.values();
    }

    public int size() {
        return entries.size();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    /**
     * Rough heap footprint, used to decide when to flush. Approximate on purpose: an exact
     * measurement would cost more than it is worth for a threshold check.
     */
    public long approximateBytes() {
        return approximateBytes.get();
    }

    private static long sizeOf(Record record) {
        return record.key().length + record.value().length + 32; // + object overhead estimate
    }
}
