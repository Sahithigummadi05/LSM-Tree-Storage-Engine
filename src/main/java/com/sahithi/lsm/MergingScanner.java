package com.sahithi.lsm;

import com.sahithi.lsm.memtable.Memtable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * A k-way merge across the memtable and every SSTable, producing records in key order.
 *
 * <p>This is what makes range queries possible, and it is the main reason to choose an LSM tree
 * over a hash-based store: every source is already sorted, so scanning a key range means merging
 * sorted streams rather than sorting anything. A hash index would have to read and sort the whole
 * dataset to answer the same question.
 *
 * <p>The merge is not a plain sorted-union, because the same key legitimately appears in several
 * sources with different values — that is how updates work here. So each source carries a
 * <b>recency rank</b>, and when several sources offer the same key the newest one wins and the
 * others are discarded. Getting that wrong doesn't produce a crash; it produces stale values in
 * scan results while point lookups stay correct, which is exactly the kind of bug that survives
 * casual testing.
 *
 * <p>Tombstones are consumed by the merge rather than returned. A deleted key must not appear in a
 * scan, and the tombstone still has to shadow older versions of that key from older sources — so
 * it wins the merge, suppresses the others, and is then dropped.
 *
 * <p>Complexity is O(n log k) for n records across k sources: each record is pushed and popped
 * from a heap of size k once.
 */
public final class MergingScanner implements Iterator<Record> {

    /** One sorted source, positioned at its current record. */
    private static final class Cursor {
        private final List<Record> records;
        private final int recency; // higher is newer
        private int position;

        Cursor(List<Record> records, int recency, byte[] startInclusive) {
            this.records = records;
            this.recency = recency;
            this.position = startInclusive == null ? 0 : seekTo(records, startInclusive);
        }

        /** Binary search for the first record with key >= target. */
        private static int seekTo(List<Record> records, byte[] target) {
            var low = 0;
            var high = records.size();
            while (low < high) {
                var mid = (low + high) >>> 1;
                if (Memtable.KEY_ORDER.compare(records.get(mid).key(), target) < 0) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            return low;
        }

        boolean exhausted() {
            return position >= records.size();
        }

        Record peek() {
            return records.get(position);
        }

        void advance() {
            position++;
        }
    }

    private final PriorityQueue<Cursor> queue;
    private final byte[] endExclusive;
    private Record nextRecord;

    /**
     * @param sources sorted record lists, ordered oldest-first (the caller's natural SSTable order)
     * @param startInclusive lower bound, or null for "from the beginning"
     * @param endExclusive upper bound, or null for "to the end"
     */
    public MergingScanner(List<List<Record>> sources, byte[] startInclusive, byte[] endExclusive) {
        this.endExclusive = endExclusive;
        // Smallest key first; on a tie the newer source first, so it wins the merge.
        this.queue = new PriorityQueue<>(
                Comparator.<Cursor, byte[]>comparing(cursor -> cursor.peek().key(), Memtable.KEY_ORDER)
                        .thenComparing(Comparator.comparingInt((Cursor c) -> c.recency).reversed()));

        for (var i = 0; i < sources.size(); i++) {
            var cursor = new Cursor(sources.get(i), i, startInclusive);
            if (!cursor.exhausted()) {
                queue.add(cursor);
            }
        }
        nextRecord = computeNext();
    }


    private Record computeNext() {
        while (!queue.isEmpty()) {
            var winner = queue.poll();
            var record = winner.peek();
            var key = record.key();

            if (endExclusive != null && Memtable.KEY_ORDER.compare(key, endExclusive) >= 0) {
                return null; // past the range; every remaining key is larger still
            }

            winner.advance();
            if (!winner.exhausted()) {
                queue.add(winner);
            }

            // Discard the same key from every older source - this record shadows them.
            while (!queue.isEmpty() && Memtable.KEY_ORDER.compare(queue.peek().peek().key(), key) == 0) {
                var shadowed = queue.poll();
                shadowed.advance();
                if (!shadowed.exhausted()) {
                    queue.add(shadowed);
                }
            }

            if (!record.tombstone()) {
                return record;
            }
            // A tombstone has done its job by suppressing older versions; it is not a result.
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        return nextRecord != null;
    }

    @Override
    public Record next() {
        if (nextRecord == null) {
            throw new NoSuchElementException();
        }
        var current = nextRecord;
        nextRecord = computeNext();
        return current;
    }

    /** Drains the scanner into a list. Convenient for tests and small ranges. */
    public List<Record> toList() {
        var results = new ArrayList<Record>();
        while (hasNext()) {
            results.add(next());
        }
        return results;
    }
}
