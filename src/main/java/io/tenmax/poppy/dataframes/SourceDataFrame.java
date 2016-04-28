package io.tenmax.poppy.dataframes;

import io.tenmax.poppy.DataColumn;
import io.tenmax.poppy.DataRow;
import io.tenmax.poppy.DataSource;

import java.util.Iterator;

public class SourceDataFrame extends BaseDataFrame{
    private final DataSource dataSource;

    public <T> SourceDataFrame(
            DataSource<T> dataSource)
    {
        super(new ExecutionContext(), dataSource.getColumns());
        this.dataSource = dataSource;
    }

    @Override
    public int getPartitionCount() {
        return dataSource.getPartitionCount();
    }

    @Override
    public Iterator<DataRow> getPartition(int index) {
        return new SourceIterator(dataSource.getPartition(index));
    }

    class SourceIterator implements Iterator<DataRow> {
        private Iterator source;

        SourceIterator(Iterator source) {
            this.source = source;
        }

        @Override
        public boolean hasNext() {
            return source.hasNext();
        }

        @Override
        public DataRow next() {
            return new SourceDataRow<>(source.next());
        }
    }

    class SourceDataRow<T> extends BaseDataRow {
        private final T data;

        SourceDataRow(T data) {
            this.data = data;
        }

        @Override
        public Object get(int index) {
            return get(columns[index].getName());
        }

        @Override
        public Object get(String name) {
            return dataSource.get(data, name);
        }
    }
}
