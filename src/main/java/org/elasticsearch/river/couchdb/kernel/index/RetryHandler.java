package org.elasticsearch.river.couchdb.kernel.index;

public class RetryHandler<T> {
    private T lastCmd;

    public T getLastCommand() {
        return lastCmd;
    }

    public void doNotRetry() {
        lastCmd = null;
    }

    public boolean shouldRetryLastAttempt() {
        return lastCmd != null;
    }

    public void newCommand(T cmd) {
        lastCmd = cmd;
    }
}
