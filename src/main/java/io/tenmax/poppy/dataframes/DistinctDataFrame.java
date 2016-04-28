package io.tenmax.poppy.dataframes;

import io.tenmax.poppy.DataColumn;
import io.tenmax.poppy.DataRow;
import io.tenmax.poppy.iterators.SequantialIterator;

import java.util.*;

public class DistinctDataFrame extends BaseDataFrame {
    private final BaseDataFrame parent;

    public DistinctDataFrame(BaseDataFrame parent, String[] distinctColumns) {
        super(new ExecutionContext(), columnsFromNames(parent, distinctColumns));

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
    public int getPartitionCount() {
        return 1;
    }

    @Override
    public Iterator<DataRow> getPartition(int index) {
        return new DistinctIterator(parent.iterator());
    }

    class DistinctIterator implements Iterator<DataRow> {
        private Iterator<DataRow> wrapped;
        private boolean ready;
        private DataRow row;

        private HashSet<List> set = new HashSet<>();

        DistinctIterator(Iterator<DataRow> wrapped) {
            this.wrapped = wrapped;
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
            DataRow row;
            List value = new ArrayList();

            while(wrapped.hasNext()) {
                row = wrapped.next();
                for (DataColumn column: columns) {
                    value.add(row.get(column.getName()));
                }

                if (!set.contains(value)) {
                    this.row = new DistinctDataRow(value);
                    this.ready = true;
                    set.add(value);
                    return;
                }
            }
            this.row = null;
            this.ready = false;
        }
    }

    class DistinctDataRow extends BaseDataRow {

        private List value;

        DistinctDataRow(List value) {
            this.value = value;
        }


        @Override
        public Object get(int index) {
            return value.get(index);
        }

    }
}
