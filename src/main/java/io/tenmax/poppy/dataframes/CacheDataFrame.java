package io.tenmax.poppy.dataframes;

import io.tenmax.poppy.DataColumn;
import io.tenmax.poppy.DataRow;
import io.tenmax.poppy.RandomAccessDataFrame;

import java.util.ArrayList;
import java.util.Iterator;

public class CacheDataFrame extends BaseDataFrame implements RandomAccessDataFrame {

    private final ArrayList<DataRow> rows = new ArrayList<>();
    private final BaseDataFrame parent;

    public CacheDataFrame(BaseDataFrame parent) {
        super(new ExecutionContext(), parent.columns);
        this.parent = parent;
        this.groupedColumns = parent.groupedColumns;
        for (DataRow row : parent) {
            rows.add(new CacheDataRow(row));
        }
    }

    @Override
    public int getPartitionCount() {
        return 1;
    }

    @Override
    public Iterator<DataRow> getPartition(int index) {
        return rows.iterator();
    }

    @Override
    public int size() {
        return rows.size();
    }

    @Override
    public DataRow getRow(int row) {
        return rows.get(row);
    }

    class CacheDataRow extends BaseDataRow {
        ArrayList value = new ArrayList();

        CacheDataRow(DataRow row) {
            for (int i=0; i<row.getColumns().length; i++) {
                value.add(row.get(i));
            }
        }

        @Override
        public Object get(int index) {
            return value.get(index);
        }
    }
}
