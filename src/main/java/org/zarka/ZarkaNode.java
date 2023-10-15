package org.zarka;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.avro.Data;
import org.zarka.model.Memtable;
import org.zarka.sstable.SSTableReader;
import org.zarka.sstable.SSTableWriter;
import org.zarka.utils.ClientHandler;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class ZarkaNode {
    private Memtable memtable;
    private SSTableWriter ssTableWriter;
    private SSTableReader ssTableReader;
    private CommitLog commitLog; // separate commitLog for each memtable
    protected static final Logger logger = LogManager.getLogger(ZarkaNode.class);

    public ZarkaNode() {
        ssTableWriter = new SSTableWriter();
        commitLog = new CommitLog();
        memtable = new Memtable(commitLog);
        memtable.applyLog();
        memtable.deleteRecoveredLog();
    }

    public void put(String key, String value) {
        logger.info("Writing data with key: " + key);
        // update the memtable and flush if nessecary
        Data data = new Data(key, value);
        memtable.put(data);
        if (memtable.exceedsThreshold()) {
            logger.info("Flushing memtable to disk asynchronously");
            ssTableWriter.writeAsync(memtable);

            // Create a new commit log for the new memtable
            commitLog = new CommitLog();

            // Replace the old memtable with a new empty one
            memtable = new Memtable(commitLog);
        }
    }

    /**
     * Check the memtable to see if the required data is present
     * 
     * @param id The id of the wheather station to read
     * @return The weather data for the given id
     */
    public String get(String key) {
        
        // check memtable first
        String value = memtable.get(key);
        if (value != null) {
            logger.info("Reading from memtable");
            return value;
        }

        // retrieve data from sstable
        if (ssTableReader == null) {
            ssTableReader = new SSTableReader();
        }
        logger.info("Reading from SSTable");
        value = ssTableReader.get(key);

        return value;
    }

    public void close() {
        ssTableWriter.shutdown();
        memtable.closeCommitLog();
    }

    protected void setMemtable(Memtable memtable) {
        this.memtable = memtable;
    }

    protected void setSSTableReader(SSTableReader ssTableReader) {
        this.ssTableReader = ssTableReader;
    }

    public static void main(String[] args) throws IOException {
        int portNumber = Integer.parseInt(args[0]);
        try (ServerSocket serverSocket = new ServerSocket(portNumber)) {
            logger.info("Node is listening on port " + portNumber);

            while (true) {
                ZarkaNode node = new ZarkaNode();

                // Wait for a client to connect
                Socket clientSocket = serverSocket.accept();
                logger.info("Client connected: " + clientSocket.getInetAddress());

                // Handle the client connection in a separate thread
                new ClientHandler(clientSocket, node).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
