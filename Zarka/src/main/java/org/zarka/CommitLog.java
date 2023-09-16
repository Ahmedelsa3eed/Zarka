package org.zarka;

import org.zarka.avro.WeatherData;
import org.zarka.model.Pair;
import org.zarka.model.WALEntry;

import java.io.*;
import java.util.List;

public class CommitLog {
    private Long entryIndex = 0L;
    private FileOutputStream file;
    private DataOutputStream wal;
    private String path;

    public CommitLog(String path) {
        this.path = path;
        try {
            // append to commitLog for now
            this.file = new FileOutputStream(path, true);
            this.wal = new DataOutputStream(new BufferedOutputStream(file));
        }
        catch (Exception e) {
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
            InputStream is = new FileInputStream(path);
            List<WALEntry> walEntries = WALEntry.deserialize(is);
            is.close();
            return walEntries;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void closeCommitLog() {
        try {
            wal.close();
            file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
