package com.sahithi.lsm;

import java.util.Arrays;

/**
 * One key-value entry, or a tombstone marking a deletion.
 *
 * <p>Deletes cannot simply remove data in an LSM tree. Older SSTables are immutable and may still
 * contain a previous value for the key, so "removing" it from the newest level would let the stale
 * value resurface on the next read. Instead a delete writes a <b>tombstone</b>: a record that says
 * "this key is gone", which shadows anything older. The tombstone itself is only discarded during
 * compaction, once nothing older can contradict it.
 */
public record Record(byte[] key, byte[] value, boolean tombstone) {

    public static Record of(byte[] key, byte[] value) {
        return new Record(key, value, false);
    }

    public static Record deletion(byte[] key) {
        return new Record(key, new byte[0], true);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Record r
                && Arrays.equals(key, r.key)
                && Arrays.equals(value, r.value)
                && tombstone == r.tombstone;
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(key) + Arrays.hashCode(value) + (tombstone ? 1 : 0);
    }

    @Override
    public String toString() {
        return (tombstone ? "DEL " : "PUT ") + new String(key) + (tombstone ? "" : "=" + new String(value));
    }
}
