package ru.java2.tasks.task2echoserver;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class EchoServerTest {

    private static final int PORT = 7777;
    private static final String[] COMMANDS = {"exit", "quit", "stop"};
    private static final int CLIENT_COUNT = 100;

    private static EchoServer server;
    private static Thread serverThread;

    @BeforeAll
    public static void startServer() throws InterruptedException {
        server = new EchoServer(new EchoServerConfig(PORT));
        serverThread = new Thread(server::start);
        serverThread.setDaemon(true);
        serverThread.setName("EchoServer-Thread");
        serverThread.start();
        Thread.sleep(500);
    }

    @AfterAll
    public static void stopServer() throws InterruptedException {
        server.shutdown();
        serverThread.join();
    }

    @Test
    public void testMultipleClientsEcho() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<Future<Boolean>> results = new ArrayList<>();

        for (int i = 0; i < CLIENT_COUNT; i++) {
            int clientId = i;
            results.add(executor.submit(() -> runClient(clientId)));
        }

        for (Future<Boolean> future : results) {
            try {
                boolean success = future.get(5, TimeUnit.SECONDS);
                assertTrue(success, "Client interaction should be successful");
            } catch (ExecutionException | TimeoutException e) {
                throw new AssertionError("Client failed: " + e.getMessage(), e);
            }
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    private boolean runClient(int id) {
        try (Socket socket = new Socket("localhost", PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            String message = "Hello from client " + id;
            out.println(message);
            String response = in.readLine();
            assertNotNull(response, "Response should not be null");
            assertEquals(message, response);

            String exitCommand = COMMANDS[ThreadLocalRandom.current().nextInt(COMMANDS.length)];
            out.println(exitCommand);
            String exitResponse = in.readLine();
            assertNotNull(exitResponse, "Exit response should not be null");

            return true;
        } catch (Exception e) {
            System.err.println("Client " + id + " error: " + e.getMessage());
            return false;
        }
    }
}