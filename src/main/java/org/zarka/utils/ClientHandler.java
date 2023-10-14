package org.zarka.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.ZarkaNode;

public class ClientHandler extends Thread {
    private Socket clientSocket;
    private ZarkaNode node;
    private static final Logger logger = LogManager.getLogger(ClientHandler.class);

    public ClientHandler(Socket socket, ZarkaNode node) {
        this.clientSocket = socket;
        this.node = node;
    }

    @Override
    public void run() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String userCommand;

            while ((userCommand = reader.readLine()) != null) {
                logger.info("Received: " + userCommand);

                // Process the received data here and prepare a response
                logger.info("Node received: " + userCommand);
                String response = "Node received: " + userCommand;
                
                // Parse user command, e.g., "add key value" or "get key"
                if (userCommand.startsWith("add")) {
                    String[] parts = userCommand.split(" ");
                    if (parts.length == 3) {
                        String key = parts[1];
                        String value = parts[2];
                        this.node.put(key, value);
                    } else {
                        response = "Invalid 'add' command format.";
                        logger.error("Invalid 'add' command format.");
                    }
                } else if (userCommand.startsWith("get")) {
                    String[] parts = userCommand.split(" ");
                    if (parts.length == 2) {
                        Long key = Long.parseLong(parts[1]);
                        response = this.node.get(key);
                        logger.info("Value for key " + key + ": " + response);
                    } else {
                        response = "Invalid 'get' command format.";
                        logger.error("Invalid 'get' command format.");
                    }
                } else {
                    response = "Invalid command. Use 'add key value' or 'get key'.";
                    logger.error("Invalid command. Use 'add key value' or 'get key'.");
                }

                // Send the response back to the client
                writer.println(response);
            }

            // Close the connection when the client disconnects
            clientSocket.close();
            logger.info("Client disconnected: " + clientSocket.getInetAddress());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
