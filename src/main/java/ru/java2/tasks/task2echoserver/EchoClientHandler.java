package ru.java2.tasks.task2echoserver;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.Socket;

/**
 * Handles a single client connection in a separate thread.
 * <p>
 * This class implements a simple echo protocol:
 * it reads input from the client, and writes the same data back.
 * If the client sends "exit", "quit", or "stop", the connection is closed gracefully.
 */
@Slf4j
@AllArgsConstructor
public class EchoClientHandler implements Runnable {
    private final Socket clientSocket;

    /**
     * Processes input from the client and echoes it back until the client sends an exit command or disconnects.
     */
    @Override
    public void run() {
        String clientAddress = clientSocket.getInetAddress() + ":" + clientSocket.getPort();
        log.info("Handling client {} in thread {}", clientSocket.getInetAddress(), Thread.currentThread().getName());

        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))) {

            String line;
            while ((line = in.readLine()) != null) {
                log.info("Client: {} : Received line: {}", clientAddress, line);

                if ("exit".equalsIgnoreCase(line.trim())
                        || "quit".equalsIgnoreCase(line.trim())
                        || "stop".equalsIgnoreCase(line.trim())
                ) {
                    out.write("Goodbye!");
                    out.newLine();
                    out.flush();
                    break;
                }


                out.write(line);
                out.newLine();
                out.flush();
            }

        } catch (IOException e) {
            log.error("Connection error with client {} : {}", clientAddress, e.getMessage());
        } finally {
            close(clientAddress);
        }
    }

    private void close(String clientAddress) {
        try {
            clientSocket.close();
            log.info("Client disconnected: {}", clientAddress);
        } catch (IOException e) {
            log.error("Error closing client socket: address: {} : {}", clientAddress, e.getMessage());
        }
    }
}
