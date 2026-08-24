package com.sahithi.lsm.wal;

import com.sahithi.lsm.Record;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * The durability mechanism: every write is appended here and forced to disk <em>before</em> the
 * write is acknowledged to the caller.
 *
 * <p>Without it, an LSM tree would lose data on every crash. Writes land in an in-memory memtable
 * and only reach disk when that memtable fills up and flushes — so a process killed between those
 * moments would silently discard everything buffered, including writes it had already told the
 * caller succeeded. The log makes the promise recoverable: on restart, replaying it rebuilds
 * exactly the memtable that was lost.
 *
 * <p><b>fsync is the load-bearing part.</b> Writing to a file is not the same as the data being on
 * disk — the OS buffers it in the page cache, and a power failure or kernel panic loses whatever
 * was still buffered. {@link FileOutputStream#getFD()}{@code .sync()} forces it down. It is also
 * by far the most expensive operation in the write path, which is exactly why databases expose it
 * as a tunable and why {@code fsyncOnWrite=false} exists here: the honest trade is durability
 * against throughput, and this engine lets you measure both sides of it rather than assume.
 *
 * <p>Each record carries a CRC32 checksum. A crash can leave a half-written record at the tail of
 * the log — the process died mid-append — and replaying those trailing bytes as if they were a
 * valid record would corrupt the store. Recovery stops at the first record that fails its
 * checksum or ends early, which is the correct interpretation: that write never completed, so it
 * was never acknowledged.
 */
public final class WriteAheadLog implements AutoCloseable {

    private final Path path;
    private final FileOutputStream fileStream;
    private final DataOutputStream out;
    private final boolean fsyncOnWrite;

    public WriteAheadLog(Path path, boolean fsyncOnWrite) throws IOException {
        this.path = path;
        this.fsyncOnWrite = fsyncOnWrite;
        Files.createDirectories(path.getParent());
        this.fileStream = new FileOutputStream(path.toFile(), true);
        this.out = new DataOutputStream(new BufferedOutputStream(fileStream, 1 << 16));
    }

    /** Appends a record and, when configured to, forces it to physical storage before returning. */
    public void append(Record record) throws IOException {
        var payload = encode(record);
        var crc = new CRC32();
        crc.update(payload);

        out.writeInt(payload.length);
        out.writeLong(crc.getValue());
        out.write(payload);
        out.flush(); // push out of the JVM's buffer into the OS

        if (fsyncOnWrite) {
            fileStream.getFD().sync(); // and out of the OS page cache onto the device
        }
    }

    private static byte[] encode(Record record) throws IOException {
        var buffer = new java.io.ByteArrayOutputStream();
        try (var data = new DataOutputStream(buffer)) {
            data.writeBoolean(record.tombstone());
            data.writeInt(record.key().length);
            data.write(record.key());
            data.writeInt(record.value().length);
            data.write(record.value());
        }
        return buffer.toByteArray();
    }

    /**
     * Replays the log from the beginning.
     *
     * <p>Stops cleanly at the first truncated or corrupt record rather than throwing: that is the
     * expected shape of a log whose process was killed mid-append, and everything before it is
     * still valid.
     */
    public static List<Record> replay(Path path) throws IOException {
        var records = new ArrayList<Record>();
        if (!Files.exists(path)) {
            return records;
        }

        try (var in = new DataInputStream(new java.io.BufferedInputStream(Files.newInputStream(path)))) {
            while (true) {
                int length;
                long expectedCrc;
                try {
                    length = in.readInt();
                    expectedCrc = in.readLong();
                } catch (EOFException e) {
                    return records; // clean end of log
                }
                if (length < 0) {
                    return records; // garbage length - treat as torn tail
                }

                var payload = new byte[length];
                try {
                    in.readFully(payload);
                } catch (EOFException e) {
                    return records; // torn record: the write never completed
                }

                var crc = new CRC32();
                crc.update(payload);
                if (crc.getValue() != expectedCrc) {
                    return records; // corrupt tail - stop, keep everything before it
                }
                records.add(decode(payload));
            }
        }
    }

    private static Record decode(byte[] payload) throws IOException {
        try (var in = new DataInputStream(new java.io.ByteArrayInputStream(payload))) {
            var tombstone = in.readBoolean();
            var key = new byte[in.readInt()];
            in.readFully(key);
            var value = new byte[in.readInt()];
            in.readFully(value);
            return new Record(key, value, tombstone);
        }
    }

    /** Discards the log. Called once its contents are safely durable in an SSTable. */
    public void truncate() throws IOException {
        out.flush();
        fileStream.getChannel().truncate(0);
        fileStream.getFD().sync();
    }

    public Path path() {
        return path;
    }

    public long sizeBytes() throws IOException {
        out.flush();
        return Files.size(path);
    }

    @Override
    public void close() throws IOException {
        out.flush();
        fileStream.getFD().sync();
        out.close();
    }
}
