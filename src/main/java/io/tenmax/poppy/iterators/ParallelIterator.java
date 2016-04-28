package io.tenmax.poppy.iterators;

import io.tenmax.poppy.DataFrame;
import io.tenmax.poppy.DataRow;
import io.tenmax.poppy.dataframes.BaseDataFrame;
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

    private final BaseDataFrame dataFrame;

    private BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    private int countDown;
    private boolean hasNext;
    private DataRow row;

    public ParallelIterator(BaseDataFrame dataFrame) {
        this.dataFrame = dataFrame;
        this.countDown = dataFrame.getPartitionCount();

        start();
    }

    public void start() {
        ExecutorService executor = Executors.newFixedThreadPool(dataFrame.getContext().getNumThreads());

        for (int i=0; i<dataFrame.getPartitionCount(); i++) {
            final int fi = i;

            executor.execute(()-> {
                try {
                    Iterator<DataRow> iter = dataFrame.getPartition(fi);
                    while (iter.hasNext()) {
                        queue.put(new Message(iter.next()));

                        if(dataFrame.getContext().isClosed()) {
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
