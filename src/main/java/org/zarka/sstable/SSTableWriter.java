package org.zarka.sstable;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.avro.WeatherData;
import org.zarka.model.Memtable;

public class SSTableWriter {
    private ExecutorService executorService;
    private static Logger logger = LogManager.getLogger(SSTableWriter.class);

    public SSTableWriter() {
        this.executorService = Executors.newSingleThreadExecutor();
    }

    public Future<Void> writeAsync(Memtable memtable) {
        return executorService.submit(() -> {
            write(memtable);
            if (!memtable.clearCommitLog()) {
                logger.error("Error clearing commit log");
            } else {
                logger.info("Cleared commit log");
            }
            return null;
        });
    }

    /**
     * Write the memtable to disk
     * format: data length (4 bytes) + data (n bytes)
     * @param memtable The memtable to write
     */
    public void write(Memtable memtable) {
        logger.info("Writing memtable to SSTable");
        SSTable ssTable = new SSTable();
        try {
            DataOutputStream dos = new DataOutputStream(
                    new FileOutputStream(ssTable.getBaseFile(), true));
            for (WeatherData data : memtable.getInOrder()) {
                byte[] buf = data.toByteBuffer().array();
                dos.writeInt(buf.length);
                dos.write(buf);
            }
            dos.close();
        } catch (FileNotFoundException e) {
            logger.error("SSTable directory not found: ", e);
        } catch (IOException e) {
            logger.error("Error writing to SSTable: ", e);
        }
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }
}
