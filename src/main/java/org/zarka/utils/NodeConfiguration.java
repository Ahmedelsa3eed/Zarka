package org.zarka.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class NodeConfiguration {
    private int numNodes;
    private int replicationFactor;
    private int quorumRead;
    private int quorumWrite;
    private String[] nodeAddresses;
    
    public NodeConfiguration() {
        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String configFile = rootPath + "node.properties";
        Properties properties = new Properties();

        try (FileInputStream fis = new FileInputStream(configFile)) {
            properties.load(fis);

            numNodes = Integer.parseInt(properties.getProperty("numNodes"));
            replicationFactor = Integer.parseInt(properties.getProperty("replicationFactor"));
            quorumRead = Integer.parseInt(properties.getProperty("quorumRead"));
            quorumWrite = Integer.parseInt(properties.getProperty("quorumWrite"));

            nodeAddresses = new String[numNodes];
            for (int i = 1; i <= numNodes; i++) {
                nodeAddresses[i - 1] = properties.getProperty("node" + i);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String[] getNodeAddresses() {
        return this.nodeAddresses;
    }

    public Integer getNumNode() {
        return this.numNodes;
    }

    public Integer getQuorumRead() {
        return this.quorumRead;
    }

    public Integer getQuorumWrite() {
        return this.quorumWrite;
    }

    public Integer getReplicationFactor() {
        return this.replicationFactor;
    }
}
