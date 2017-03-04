package org.skife.memcake.connection;

public class StatusException extends Exception {
    private final char status;

    public StatusException(char status, String message) {
        super(message);
        this.status = status;
    }

    public char getStatus() {
        return status;
    }
}