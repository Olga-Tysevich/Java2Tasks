package ru.java2.tasks.task2echoserver;

/**
 * Represents the configuration for the EchoServer.
 *
 * @param port the TCP port the server will listen on
 */
public record EchoServerConfig(int port) {

    public EchoServerConfig {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid port: " + port);
        }
    }

}
