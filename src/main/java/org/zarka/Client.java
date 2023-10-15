package org.zarka;

import java.io.*;
import java.net.Socket;
import java.util.Random;

import org.zarka.utils.NodeConfiguration;

public class Client {

    public static void main(String[] args) {
        NodeConfiguration config = new NodeConfiguration();
        String[] nodeAddresses = config.getNodeAddresses();
        System.out.println("node: " + nodeAddresses[0]);
        if (nodeAddresses.length == 0) {
            System.err.println("No nodes found in the configuration.");
            return;
        }

        // Randomly select a node
        Random random = new Random();
        int randomIndex = random.nextInt(nodeAddresses.length);
        String selectedNodeAddress = nodeAddresses[randomIndex];
        System.out.println("Selected node: " + selectedNodeAddress);

        // Parse the selected node's host and port
        String[] parts = selectedNodeAddress.split(":");
        if (parts.length != 2) {
            System.err.println("Invalid node address format: " + selectedNodeAddress);
            return;
        }
        String nodeHost = parts[0];
        int nodePort = Integer.parseInt(parts[1]);

        try (Socket socket = new Socket(nodeHost, nodePort);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader userInput = new BufferedReader(new InputStreamReader(System.in))) {

            System.out.println("Zarka Client");
            System.out.println("Commands: add <key> <value>, get <key>");

            String userInputStr;
            while ((userInputStr = userInput.readLine()) != null) {
                out.println(userInputStr); // Send the user's input to the server

                String serverResponse = in.readLine(); // Receive the server's response
                System.out.println("Server Response: " + serverResponse);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
