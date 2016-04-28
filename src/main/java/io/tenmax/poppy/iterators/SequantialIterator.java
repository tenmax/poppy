package io.tenmax.poppy.iterators;

import io.tenmax.poppy.DataRow;
import io.tenmax.poppy.dataframes.BaseDataFrame;

import java.util.Iterator;

public class SequantialIterator implements Iterator<DataRow> {
    private final BaseDataFrame dataFrame;
    private int top;
    private int partitionCount;
    private Iterator<DataRow> iterator;

    public SequantialIterator(BaseDataFrame dataFrame) {
        this.dataFrame = dataFrame;
        this.partitionCount = dataFrame.getPartitionCount();
    }

    @Override
    public boolean hasNext() {
        while (true) {
            if(dataFrame.getContext().isClosed()) {
                return false;
            }

            if(iterator != null && iterator.hasNext()) {
                return true;
            }

            if (top >= partitionCount) {
                return false;
            }
            iterator = dataFrame.getPartition(top++);
        }
    }

    @Override
    public DataRow next() {
        return iterator.next();
    }
}
