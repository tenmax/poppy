package io.tenmax.poppy.datasources;

import io.tenmax.poppy.DataColumn;
import io.tenmax.poppy.DataSource;
import io.tenmax.poppy.exceptions.ColumnNotFoundException;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.util.ArrayList;

public abstract class ReflectionDataSource<T> implements DataSource<T>{
    private final DataColumn[] columns;

    public ReflectionDataSource(Class<T> clazz) {
        this.columns = schemaFromClass(clazz);
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
    }
}
