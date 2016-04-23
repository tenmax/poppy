package io.tenmax.poppy.iterators;

import io.tenmax.poppy.DataRow;
import io.tenmax.poppy.dataframes.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class ParallelIterator  implements Iterator<DataRow> {
    private static Logger logger = LoggerFactory.getLogger(ParallelIterator.class);

    private final ExecutionContext context;
    private final Iterator<DataRow>[] iterators;
    private BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    private int countDown;
    private boolean hasNext;
    private DataRow row;

    public ParallelIterator(ExecutionContext context, Iterator<DataRow>... iterators) {
        this.context = context;
        this.iterators = iterators;
        this.countDown = iterators.length;

        start();
    }

    public void start() {
        ExecutorService executor = Executors.newFixedThreadPool(context.getNumThreads());

        for (Iterator<DataRow> iter : iterators) {
            executor.execute(()-> {
                try {
                    while (iter.hasNext()) {
                        queue.put(new Message(iter.next()));

                        if(context.isClosed()) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error occured", e);
                } finally {
                    queue.add(Message.END_OF_MESSAGE);
                }
            });
        }

        // Shutdown while all task handled
        executor.shutdown();
    }

    @Override
    public boolean hasNext() {
        if (!hasNext) {
            findNext();
        }

        return hasNext;
    }

    @Override
    public DataRow next() {
        if (!hasNext) {
            findNext();
        }

        if (hasNext) {
            hasNext = false;
            return row;
        } else {
            return null;
        }
    }

    public void findNext() {
        hasNext = false;
        while (countDown > 0) {

            Message message = null;
            try {
                message = queue.take();

                if (message == Message.END_OF_MESSAGE) {
                    countDown--;
                } else {
                    row = message.row;
                    hasNext = true;
                    break;
                }
            } catch (InterruptedException e) {

            }
        }
    }


    static class Message {
        static Message END_OF_MESSAGE = new Message(null);

        DataRow row;

        Message(DataRow row) {
            this.row = row;
        }
    }
}
