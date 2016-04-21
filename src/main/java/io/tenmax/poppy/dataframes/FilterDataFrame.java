package io.tenmax.poppy.dataframes;

import io.tenmax.poppy.DataColumn;
import io.tenmax.poppy.DataRow;

import java.util.Iterator;
import java.util.function.Predicate;

public class FilterDataFrame extends BaseDataFrame {
    private final BaseDataFrame parent;
    private final Predicate<DataRow> predicate;

    public FilterDataFrame(BaseDataFrame parent, Predicate<DataRow> predicate) {
        super(parent.getColumns());
        this.parent = parent;
        this.predicate = predicate;
    }

    @Override
    int getPartitionCount() {
        return parent.getPartitionCount();
    }

    @Override
    Iterator<DataRow> getPartition(int index) {
        return new FilterIterator(parent.getPartition(index));
    }

    class FilterIterator implements Iterator<DataRow>, DataRow {
        private Iterator<DataRow> wrapped;
        private DataRow row;
        private boolean ready;

        FilterIterator(Iterator<DataRow> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public Object get(int index) {
            return row.get(index);
        }

        @Override
        public Object get(String name) {
            return row.get(name);
        }

        @Override
        public boolean hasNext() {
            if (!ready) {
                findNext();
            }
            return row != null;
        }

        @Override
        public DataRow next() {
            if (!ready) {
                findNext();
            }

            ready = false;
            return row;
        }

        private void findNext() {
            while(wrapped.hasNext()) {
                row = wrapped.next();
                if (predicate.test(row)) {
                    ready = true;
                    return;
                }
            }
            row = null;
            ready = false;
        }
    }
}
