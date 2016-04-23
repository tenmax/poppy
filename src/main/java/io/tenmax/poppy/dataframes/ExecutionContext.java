package io.tenmax.poppy.dataframes;

public class ExecutionContext {
    private boolean closed;
    private int numThreads = 1;

    public int getNumThreads() {
        return numThreads;
    }

    public void setNumThreads(int numThreads) {
        if(numThreads <= 0) {
            throw new IllegalArgumentException("numThreads should be greater than 0");
        }

        this.numThreads = numThreads;
    }

    public void close() {
        this.closed = true;
    }

    public boolean isClosed() {
        return closed;
    }
}
