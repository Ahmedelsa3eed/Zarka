package org.zarka;

import org.zarka.model.Pair;
import org.zarka.model.WALEntry;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class CommitLog {
    private final String path;
    private Long entryIndex = 0L;
    private FileOutputStream file;
    DataOutputStream wal;

    public CommitLog(String path) {
        this.path = path;
        try {
            this.file = new FileOutputStream(path);
            this.wal = new DataOutputStream(new BufferedOutputStream(file));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void appendLog(Integer key, String value) {
        try {
            WALEntry entry = new WALEntry(
                   entryIndex++,
                   new Pair(key, value).serialize(),
                   System.currentTimeMillis());
            entry.serialize(wal);
            wal.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<WALEntry> readAll() {
        // TODO: read all entries from commit log
        return null;
    }
}
