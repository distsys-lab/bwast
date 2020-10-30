package bftsmart.communication.server;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * This class count traffic of InputStream.
 */
public abstract class TrafficCountInputStream extends InputStream {
    private final InputStream in;
    private long traffic = 0;

    /**
     *
     * @param in count this
     * @param executor for schedule
     * @param name name of counter
     * @param checkInterval interval milliseconds
     */
    TrafficCountInputStream(InputStream in, ScheduledExecutorService executor, final String name, long checkInterval) {
        this.in = in;
        executor.scheduleAtFixedRate(() -> {
            doNotify(name, traffic);
            traffic = 0;
        }, checkInterval, checkInterval, TimeUnit.MILLISECONDS);
    }

    /**
     * Do notify each time that elapsed checkInterval milliseconds
     * @param name name of counter
     * @param traffic traffic of InputStream [byte per checkInterval]
     */
    abstract void doNotify(final String name, long traffic);

    @Override
    public int available() throws IOException {
        return in.available();
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        in.mark(readlimit);
    }

    @Override
    public boolean markSupported() {
        return in.markSupported();
    }

    @Override
    public int read() throws IOException {
        synchronized (this) {
            traffic++;
        }
        return in.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int num =  in.read(b, off, len);
        synchronized (this) {
            traffic += num;
        }
        return num;
    }

    @Override
    public synchronized void reset() throws IOException {
        traffic = 0;
        in.reset();
    }
}
