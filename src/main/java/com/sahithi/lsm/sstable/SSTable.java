package com.sahithi.lsm.sstable;

import com.sahithi.lsm.Record;
import com.sahithi.lsm.bloom.BloomFilter;
import com.sahithi.lsm.memtable.Memtable;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * An immutable sorted run of records on disk.
 *
 * <p>Immutability is the design decision everything else follows from. Because a file is never
 * modified after it is written, writes never seek, readers never lock, and a crash can never leave
 * a half-updated file — the only failure mode is a file that was never completely written, which
 * is detectable and discardable. Updating a key doesn't touch the old file at all; it writes a
 * newer record elsewhere that shadows it, and compaction reclaims the space later.
 *
 * <p><b>File layout</b> (all big-endian):
 *
 * <pre>
 *   [record 0][record 1]...[record n-1]     data block, sorted by key
 *   [index entry 0]...[index entry m-1]     sparse index: every Nth key -> its byte offset
 *   [bloom filter]
 *   [indexOffset:long][bloomOffset:long][recordCount:int][MAGIC:int]   fixed 24-byte footer
 * </pre>
 *
 * <p>The index is <b>sparse</b> — one entry per {@value #INDEX_INTERVAL} records rather than one
 * per record. A dense index would be as large as the keys themselves and defeat the purpose of
 * keeping it in memory. Sparse means a lookup binary-searches the index to find the nearest
 * preceding anchor, then scans forward at most {@value #INDEX_INTERVAL} records. That bounded scan
 * is the deliberate trade: a little I/O per lookup to keep the index small enough to stay resident.
 *
 * <p>The footer is written last and carries a magic number. A file without a valid footer was
 * never finished — a crash during flush — and can be safely deleted, because the data it held is
 * still in the write-ahead log.
 */
public final class SSTable implements AutoCloseable {

    private static final int MAGIC = 0x4C534D54; // "LSMT"
    private static final int FOOTER_BYTES = 8 + 8 + 4 + 4;
    static final int INDEX_INTERVAL = 64;

    private record IndexEntry(byte[] key, long offset) {
    }

    private final Path path;
    private final List<IndexEntry> index;
    private final BloomFilter bloom;
    private final int recordCount;
    private final byte[] data;

    private SSTable(Path path, List<IndexEntry> index, BloomFilter bloom, int recordCount, byte[] data) {
        this.path = path;
        this.index = index;
        this.bloom = bloom;
        this.recordCount = recordCount;
        this.data = data;
    }

    // ------------------------------------------------------------------
    // Writing
    // ------------------------------------------------------------------

    /** Writes records (which must already be in key order) to a new immutable file. */
    public static void write(Path path, Collection<Record> records) throws IOException {
        Files.createDirectories(path.getParent());
        var bloom = BloomFilter.create(Math.max(records.size(), 1), 0.01);
        var index = new ArrayList<IndexEntry>();

        var dataBuffer = new java.io.ByteArrayOutputStream();
        try (var data = new DataOutputStream(dataBuffer)) {
            var position = 0L;
            var i = 0;
            for (var record : records) {
                if (i % INDEX_INTERVAL == 0) {
                    index.add(new IndexEntry(record.key(), position));
                }
                var encoded = encode(record);
                data.write(encoded);
                position += encoded.length;
                bloom.add(record.key());
                i++;
            }
        }
        var dataBytes = dataBuffer.toByteArray();

        // Write to a temporary name and rename into place, so a crash mid-write can never leave a
        // partially written file under the real name. Rename is atomic on POSIX filesystems.
        var temp = path.resolveSibling(path.getFileName() + ".tmp");
        try (var fileStream = new FileOutputStream(temp.toFile());
                var out = new DataOutputStream(new BufferedOutputStream(fileStream, 1 << 16))) {
            out.write(dataBytes);

            var indexOffset = (long) dataBytes.length;
            out.writeInt(index.size());
            for (var entry : index) {
                out.writeInt(entry.key().length);
                out.write(entry.key());
                out.writeLong(entry.offset());
            }

            var bloomOffset = indexOffset + indexSizeBytes(index);
            bloom.writeTo(out);

            out.writeLong(indexOffset);
            out.writeLong(bloomOffset);
            out.writeInt(records.size());
            out.writeInt(MAGIC);

            out.flush();
            fileStream.getFD().sync(); // the file must be durable before the rename publishes it
        }
        Files.move(temp, path, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    private static long indexSizeBytes(List<IndexEntry> index) {
        var size = 4L; // entry count
        for (var entry : index) {
            size += 4 + entry.key().length + 8;
        }
        return size;
    }

    private static byte[] encode(Record record) throws IOException {
        var buffer = new java.io.ByteArrayOutputStream();
        try (var out = new DataOutputStream(buffer)) {
            out.writeBoolean(record.tombstone());
            out.writeInt(record.key().length);
            out.write(record.key());
            out.writeInt(record.value().length);
            out.write(record.value());
        }
        return buffer.toByteArray();
    }

    // ------------------------------------------------------------------
    // Reading
    // ------------------------------------------------------------------

    /**
     * Opens an SSTable.
     *
     * @throws IOException if the file has no valid footer, meaning it was never completely written
     */
    public static SSTable open(Path path) throws IOException {
        var bytes = Files.readAllBytes(path);
        if (bytes.length < FOOTER_BYTES) {
            throw new IOException("Truncated SSTable (smaller than footer): " + path);
        }

        var footer = new DataInputStream(new java.io.ByteArrayInputStream(
                bytes, bytes.length - FOOTER_BYTES, FOOTER_BYTES));
        var indexOffset = footer.readLong();
        var bloomOffset = footer.readLong();
        var recordCount = footer.readInt();
        if (footer.readInt() != MAGIC) {
            throw new IOException("Missing SSTable magic - file was not finished: " + path);
        }

        var indexStream = new DataInputStream(new java.io.ByteArrayInputStream(
                bytes, (int) indexOffset, (int) (bloomOffset - indexOffset)));
        var entryCount = indexStream.readInt();
        var index = new ArrayList<IndexEntry>(entryCount);
        for (var i = 0; i < entryCount; i++) {
            var key = new byte[indexStream.readInt()];
            indexStream.readFully(key);
            index.add(new IndexEntry(key, indexStream.readLong()));
        }

        var bloomStream = new DataInputStream(new java.io.ByteArrayInputStream(
                bytes, (int) bloomOffset, (int) (bytes.length - FOOTER_BYTES - bloomOffset)));
        var bloom = BloomFilter.readFrom(bloomStream);

        var data = new byte[(int) indexOffset];
        System.arraycopy(bytes, 0, data, 0, (int) indexOffset);

        return new SSTable(path, index, bloom, recordCount, data);
    }

    /**
     * Looks up a key.
     *
     * @return the record (which may be a tombstone), or null if this table does not contain the key
     */
    public Record get(byte[] key) throws IOException {
        // Cheapest check first: if the bloom filter says no, the key is definitely not here and we
        // avoid touching the data entirely.
        if (!bloom.mightContain(key)) {
            return null;
        }

        var anchor = floorIndexEntry(key);
        if (anchor < 0) {
            return null; // key sorts before the first record in this table
        }

        var position = (int) index.get(anchor).offset();
        var scanned = 0;
        while (position < data.length && scanned <= INDEX_INTERVAL) {
            var in = new DataInputStream(new java.io.ByteArrayInputStream(
                    data, position, data.length - position));
            var tombstone = in.readBoolean();
            var candidateKey = new byte[in.readInt()];
            in.readFully(candidateKey);
            var value = new byte[in.readInt()];
            in.readFully(value);

            var comparison = Memtable.KEY_ORDER.compare(candidateKey, key);
            if (comparison == 0) {
                return new Record(candidateKey, value, tombstone);
            }
            if (comparison > 0) {
                return null; // sorted order means we have passed where it would be
            }

            position += 1 + 4 + candidateKey.length + 4 + value.length;
            scanned++;
        }
        return null;
    }

    /** Index of the last index entry whose key is <= the search key, or -1. */
    private int floorIndexEntry(byte[] key) {
        var low = 0;
        var high = index.size() - 1;
        var result = -1;
        while (low <= high) {
            var mid = (low + high) >>> 1;
            if (Memtable.KEY_ORDER.compare(index.get(mid).key(), key) <= 0) {
                result = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return result;
    }

    /** Every record in key order - used by compaction to merge tables. */
    public List<Record> readAll() throws IOException {
        var records = new ArrayList<Record>(recordCount);
        var position = 0;
        while (position < data.length) {
            var in = new DataInputStream(new java.io.ByteArrayInputStream(
                    data, position, data.length - position));
            var tombstone = in.readBoolean();
            var key = new byte[in.readInt()];
            in.readFully(key);
            var value = new byte[in.readInt()];
            in.readFully(value);
            records.add(new Record(key, value, tombstone));
            position += 1 + 4 + key.length + 4 + value.length;
        }
        return records;
    }

    public Path path() {
        return path;
    }

    public int recordCount() {
        return recordCount;
    }

    public BloomFilter bloomFilter() {
        return bloom;
    }

    @Override
    public void close() {
        // Data is held in memory for this implementation; nothing to release.
    }
}
