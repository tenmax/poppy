package io.tenmax.poppy.datasources;

import io.tenmax.poppy.DataColumn;
import io.tenmax.poppy.DataSource;
import io.tenmax.poppy.exceptions.ColumnNotFoundException;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * SimpleDataSource use the java reflection to define the columns. And using
 * the Java Bean conversion to get the value of a column.
 *
 * @param <T>
 */
public class SimpleDataSource<T> implements DataSource<T> {
    private final Iterable<T>[] iterables;
    private final DataColumn[] columns;

    public SimpleDataSource(Class<T> clazz, Iterable<T>... iterables) {
        this.iterables = iterables;
        this.columns = schemaFromClass(clazz);
    }

    @Override
    public int getPartitionCount() {
        return iterables.length;
    }

    @Override
    public Iterator<T> getPartition(int index) {
        return iterables[index].iterator();
    }

    @Override
    public DataColumn[] getColumns() {
        return columns;
    }

    @Override
    public Object get(T data, String columnName) {
        try {
            return PropertyUtils.getProperty(data, columnName);
        } catch (Exception e) {
            throw new ColumnNotFoundException(columnName);
        }
    };

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
}
