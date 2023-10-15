package org.zarka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.avro.Data;
import org.zarka.model.WALEntry;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/*
 * Commit Log tracks every write operation into the system. The aim of the commit log is to be able to
 * successfully recover data that was not stored to disk via the Memtable.
 */
public class CommitLog {
    private Long entryIndex = 1L;
    private FileOutputStream fos;
    private DataOutputStream dos;
    private String baseName;
    private File logFile; // log file for current memtable
    private static Logger logger = LogManager.getLogger(CommitLog.class);

    public CommitLog() {
        baseName = "logs/commitLog_" + System.currentTimeMillis();
        logFile = new File(baseName + ".allocating");
        try {
            if (!logFile.exists()) {
                logger.info("Creating new allocating commitLog segment");
                logFile.createNewFile();
            }
            fos = new FileOutputStream(logFile.getAbsolutePath(), true);
            dos = new DataOutputStream(new BufferedOutputStream(fos));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void appendLog(Data data) {
        try {
            WALEntry entry = new WALEntry(
                    entryIndex++,
                    data,
                    System.currentTimeMillis());
            entry.serialize(dos); // append into commit log
            fos.flush(); 
        } catch (IOException e) {
            logger.error("Error appending to commit log", e);
        }
    }

    /**
     * Read and recover entries from commit log files.
     */
    public List<WALEntry> recover() {
        List<WALEntry> recoveredEntries = new ArrayList<>();
        
        File commitLogDir = new File("logs/");
        File[] commitLogFiles = commitLogDir.listFiles((dir, name) -> name.startsWith("commitLog_"));
        
        if (commitLogFiles != null && commitLogFiles.length > 0) {
            Arrays.sort(commitLogFiles, Comparator.comparing(File::getName));
            
            for (File logFile : commitLogFiles) {
                try (InputStream is = new FileInputStream(logFile)) {
                    List<WALEntry> walEntries = WALEntry.deserialize(is);
                    recoveredEntries.addAll(walEntries);
                } catch (IOException e) {
                    logger.error("Error recovering commit log", e);
                }
            }
        }

        return recoveredEntries;
    }

    public void deleteRecoveredLog() {
        File commitLogDir = new File("logs/");
        File[] commitLogFiles = commitLogDir.listFiles((dir, name) -> name.startsWith("commitLog_") && !name.equals(logFile.getName()));
        
        if (commitLogFiles != null && commitLogFiles.length > 0) {
            for (File logFile : commitLogFiles) {
                if (!logFile.delete()) {
                    logger.error("Error deleting commit log file: " + logFile.getName());
                }
            }
        }
    }

    public void close() {
        try {
            dos.close();
            fos.close();
        } catch (IOException e) {
            logger.error("Error closing commit log", e);
        }
    }

    public boolean clear() {
        if (logFile.exists()) {
            try {
                dos.close();
                fos.close();
                return logFile.delete();
            } catch (IOException e) {
                logger.error("Error clearing commit log", e);
                return false;
            }
        }
        return false;
    }
}
