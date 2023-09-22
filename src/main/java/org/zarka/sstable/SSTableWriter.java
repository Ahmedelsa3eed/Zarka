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
            }
            return null;
        });
    }

    public void write(Memtable memtable) {
        logger.info("Writing memtable to SSTable");
        SSTable ssTable = new SSTable();
        try {
            DataOutputStream dos = new DataOutputStream(
                new FileOutputStream(ssTable.getBaseFileName(), true));
            for (WeatherData data: memtable.getInOrder())
                dos.write(data.toByteBuffer().array());
            dos.flush();
            dos.close();
        }
        catch (FileNotFoundException e) {
            logger.error("SSTable directory not found: ", e);
        }
        catch (IOException e) {
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
