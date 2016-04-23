package io.tenmax.poppy.dataframes;

import io.tenmax.poppy.DataRow;

import java.util.Iterator;
import java.util.function.Consumer;

public class PeekDataFrame extends BaseDataFrame {
    private final BaseDataFrame parent;
    private final Consumer<DataRow> consumer;

    public PeekDataFrame(BaseDataFrame parent, Consumer<DataRow> consumer) {
        super(parent.context, parent.getColumns());
        this.parent = parent;
        this.consumer = consumer;
    }

    @Override
    int getPartitionCount() {
        return parent.getPartitionCount();
    }

    @Override
    Iterator<DataRow> getPartition(int index) {
        return new PeekIterator(parent.getPartition(index));
    }

    class PeekIterator implements Iterator<DataRow> {
        private Iterator<DataRow> wrapped;

        PeekIterator(Iterator<DataRow> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public boolean hasNext() {
            return wrapped.hasNext();
        }

        @Override
        public DataRow next() {
            DataRow row = wrapped.next();
            consumer.accept(row);
            return row;
        }
    }
}
