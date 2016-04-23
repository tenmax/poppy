package io.tenmax.poppy.dataframes;

import io.tenmax.poppy.DataColumn;
import io.tenmax.poppy.DataRow;
import io.tenmax.poppy.DataSource;
import io.tenmax.poppy.exceptions.ColumnNotFoundException;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.BiFunction;

public class SourceDataFrame extends BaseDataFrame{
    private final DataSource dataSource;
    private final BiFunction mapper;

    public <T> SourceDataFrame(
            DataSource<T> dataSource,
            Class<T> clazz)
    {
        super(new ExecutionContext(), schemaFromClass(clazz));
        this.dataSource = dataSource;
        this.mapper = (data, column) -> {
            String columnName = (String)column;

            try {
                return PropertyUtils.getProperty(data, columnName);
            } catch (Exception e) {
                throw new ColumnNotFoundException(columnName);
            }
        };
    }

    public <T> SourceDataFrame(
            DataSource<T> dataSource,
            DataColumn[] columns,
            BiFunction<T, String, Object> mapper)
    {
        super(new ExecutionContext(), columns);
        this.dataSource = dataSource;
        this.mapper = mapper;
    }

    private static DataColumn[] schemaFromClass(Class clazz) {
        PropertyDescriptor[] props = PropertyUtils.getPropertyDescriptors(clazz);
        ArrayList<DataColumn> columns = new ArrayList<>();

        for (PropertyDescriptor prop : props) {

            if(prop.getName().equals("class")) {
                continue;
            }
            columns.add(new DataColumn(prop.getName(), prop.getPropertyType()));
        }

        return columns.toArray(new DataColumn[0]);
    }

    @Override
    int getPartitionCount() {
        return dataSource.getPartitionCount();
    }

    @Override
    Iterator<DataRow> getPartition(int index) {
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

    class SourceDataRow<T> implements DataRow {
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
            return mapper.apply(data, name);
        }
    }
}
