package io.tenmax.poppy.dataframes;

import io.tenmax.poppy.DataColumn;
import io.tenmax.poppy.DataRow;

import java.util.*;

public class DistinctDataFrame extends BaseDataFrame {
    private final BaseDataFrame parent;

    public DistinctDataFrame(BaseDataFrame parent, String[] distinctColumns) {
        super(columnsFromNames(parent, distinctColumns));

        this.parent = parent;
    }

    private static DataColumn[] columnsFromNames(BaseDataFrame parent, String[] distinctColumns) {
        DataColumn[] dataColumns = new DataColumn[distinctColumns.length];
        int i = 0;

        for (String columnName : distinctColumns) {
            dataColumns[i++] = parent.getColumn(columnName);
        }

        return  dataColumns;
    }

    @Override
    int getPartitionCount() {
        return 1;
    }

    @Override
    Iterator<DataRow> getPartition(int index) {
        int count = parent.getPartitionCount();
        ArrayList<Iterator> iterators = new ArrayList<>();

        for (int i=0; i<count; i++) {
            Iterator<DataRow> partition = parent.getPartition(i);
            iterators.add(partition);
        }

        SequantialIterator iterator = new SequantialIterator(iterators.toArray(new Iterator[0]));

        return new DistinctIterator(iterator);
    }

    class DistinctIterator implements Iterator<DataRow>, DataRow {
        private Iterator<DataRow> wrapped;
        private boolean ready;
        private List value;
        private HashSet<List> set = new HashSet<>();

        DistinctIterator(Iterator<DataRow> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public Object get(int index) {
            return value.get(index);
        }

        @Override
        public Object get(String name) {
            return value.get(columnsMap.get(name));
        }

        @Override
        public boolean hasNext() {
            if (!ready) {
                findNext();
            }

            return value != null;
        }

        @Override
        public DataRow next() {
            if (!ready) {
                findNext();
            }

            ready = false;
            return value == null ? null : this;
        }

        private void findNext() {
            DataRow row;
            List value = new ArrayList();

            while(wrapped.hasNext()) {
                row = wrapped.next();
                for (DataColumn column: columns) {
                    value.add(row.get(column.getName()));
                }

                if (!set.contains(value)) {
                    this.value = value;
                    this.ready = true;
                    set.add(value);
                    return;
                }
            }
            this.value = null;
            this.ready = false;
        }
    }
}
