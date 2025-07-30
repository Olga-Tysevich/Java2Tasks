package ru.java2.tasks.task2echoserver;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A multithreaded TCP echo server.
 * <p>
 * This server listens for incoming connections and handles each client
 * using a separate virtual thread. Clients are processed by {@link EchoClientHandler}.
 */
@Slf4j
public class EchoServer {
    /**
     * Server configuration object holding the port
     *
     * @see EchoServerConfig
     **/
    private final EchoServerConfig conf;
    /**
     * Thread pool for handling clients
     */
    private final ExecutorService clientPool;
    /**
     * Server socket listening for incoming connections
     */
    private volatile ServerSocket serverSocket;
    /**
     * Tracks whether the server is currently running
     */
    private final AtomicBoolean running = new AtomicBoolean(false);
    /**
     * Grace period to allow threads to finish on shutdown
     */
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 30;

    /**
     * Constructs an EchoServer using the specified configuration.
     *
     * @param conf the server configuration
     */
    public EchoServer(EchoServerConfig conf) {
        this.conf = conf;
        this.clientPool = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("echo-client-", 0).factory()
        );
    }

    /**
     * Starts the server. Accepts client connections and dispatches them to handler threads.
     * <p>
     * If the server is already running, this method does nothing.
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("Server is already running. Thread: {}", Thread.currentThread().getName());
            return;
        }

        try (ServerSocket server = new ServerSocket(conf.port())) {
            this.serverSocket = server;
            log.info("Echo server started on port {}. Thread: {}", server.getLocalPort(), Thread.currentThread().getName());

            while (running.get()) {
                try {
                    Socket clientSocket = server.accept();
                    clientPool.submit(new EchoClientHandler(clientSocket));
                } catch (IOException e) {
                    if (running.get()) {
                        log.error("Error accepting client: {}", e.getMessage());
                    }
                }
            }

        } catch (IOException e) {
            log.error("Failed to start server: {}", e.getMessage());
        } finally {
            cleanup();
        }
    }

    /**
     * Checks if the server is currently running.
     *
     * @return {@code true} if running, otherwise {@code false}
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Initiates a graceful shutdown of the server.
     * <p>
     * This closes the server socket and stops accepting new clients.
     */
    public void shutdown() {
        if (!running.compareAndSet(true, false)) {
            log.warn("Shutdown called with Thread: {}, but server already stopped.", Thread.currentThread().getName());
            return;
        }

        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
                log.info("Server socket closed with Thread: {}.", Thread.currentThread().getName());
            }
        } catch (IOException e) {
            log.error("Error closing server socket: {}", e.getMessage());
        }

        log.info("Shutdown initiated. Waiting for client threads to finish...");
    }

    /**
     * Cleans up resources and shuts down client handling threads.
     */
    private void cleanup() {
        log.info("Graceful shutdown started. Allowing up to {} seconds for threads to complete.", SHUTDOWN_TIMEOUT_SECONDS);

        clientPool.shutdown();

        try {
            if (!clientPool.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("Not all threads finished in time. Forcing shutdown.");
                clientPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Shutdown interrupted. Forcing immediate termination.");
            clientPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("Server resources cleaned up. Thread: {}", Thread.currentThread().getName());
    }
}