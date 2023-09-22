package org.zarka;

import org.zarka.avro.WeatherData;
import org.zarka.model.WALEntry;

import java.io.*;
import java.util.List;

public class CommitLog {
    private Long entryIndex = 0L;
    private FileOutputStream file;
    private DataOutputStream wal;
    private final String baseName = "logs/commitLog";
    private File allocatingSegment;
    private File availableSegment;

    public CommitLog() {
        allocatingSegment = new File(baseName + ".allocating");
        availableSegment = new File(baseName + ".available");
        try {
            if (!allocatingSegment.exists()) {
                allocatingSegment.createNewFile();
            }
            else {
                if (allocatingSegment.renameTo(availableSegment)) {
                    System.out.println("Segment renamed successfully");
                }
            }
            file = new FileOutputStream(allocatingSegment.getAbsolutePath(), true);
            wal = new DataOutputStream(new BufferedOutputStream(file));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void appendLog(WeatherData data) {
        try {
            WALEntry entry = new WALEntry(
                   entryIndex++,
                   data,
                   System.currentTimeMillis());
            // append into commit log
            entry.serialize(wal);
            // TODO: remove flush() later
            wal.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read all entries from commit log
     * */
    public List<WALEntry> readAll() {
        try {
            InputStream is = new FileInputStream(baseName + ".allocating");
            List<WALEntry> walEntries = WALEntry.deserialize(is);
            is.close();
            return walEntries;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            wal.close();
            file.close();
            if (availableSegment.exists()) {
                availableSegment.delete();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean clear() {
        if (allocatingSegment.exists()) {
            try {
                wal.close();
                file.close();
                return allocatingSegment.delete();
            } catch (IOException e) {
                return false;
            }
        }
        return false;
    }
}
