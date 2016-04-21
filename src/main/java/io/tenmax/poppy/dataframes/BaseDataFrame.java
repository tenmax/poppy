package io.tenmax.poppy.dataframes;

import io.tenmax.poppy.*;
import io.tenmax.poppy.exceptions.ColumnNotFoundException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

abstract public class BaseDataFrame implements DataFrame{
    protected final DataColumn[] columns;
    protected final HashMap<String, Integer> columnsMap;
    protected DataColumn[] groupedColumns = new DataColumn[0];

    public BaseDataFrame(DataColumn[] columns) {
        this.columns = columns;
        this.columnsMap = new HashMap<>();

        int i = 0;
        for (DataColumn column: columns) {
            columnsMap.put(column.getName(), i++);
        }
    }

    @Override
    public DataColumn[] getColumns() {
        return columns;
    }

    @Override
    public DataColumn getColumn(String name) {
        Integer index = columnsMap.get(name);
        if(index == null) {
            throw new ColumnNotFoundException(name);
        }

        return columns[index];
    }

    @Override
    public DataColumn getColumn(int index) {
        return columns[index];
    }

    @Override
    public DataFrame project(String... columns) {
        return new ProjectDataFrame(this, columns);
    }

    @Override
    public DataFrame project(ProjectColumnSpec... columns) {
        return new ProjectDataFrame(this, columns);
    }

    @Override
    public DataFrame groupby(String... groupedColumns) {
        DataColumn[] gc = new DataColumn[groupedColumns.length];

        for (int i=0; i<groupedColumns.length; i++) {
            gc[i] = getColumn(groupedColumns[i]);
        }
        this.groupedColumns = gc;

        return this;
    }

    @Override
    public DataFrame aggregate(AggregateColumnSpec... specs) {
        return new AggregateDataFrame(this, specs);
    }

    @Override
    public DataFrame sort(String... columns) {
        SortSpec[] specs = new SortSpec[columns.length];
        for (int i=0; i<columns.length; i++) {
            specs[i] = new SortSpec(columns[i], SortSpec.Order.ASC);
        }
        return sort(specs);
    }

    @Override
    public DataFrame sort(SortSpec... specs) {
        return new SortDataFrame(this, specs);
    }

    @Override
    public DataFrame distinct(String... columns) {
        return new DistinctDataFrame(this, columns);
    }

    @Override
    public DataFrame filter(Predicate<DataRow> predicate) {
        return new FilterDataFrame(this, predicate);
    }

    @Override
    public void print() {
        Arrays.stream(columns).forEach(column ->{
            System.out.printf("%s\t", column.getName());
        });
        System.out.println();

        forEach((row) -> {
            for (int i = 0; i < columns.length; i++) {
                System.out.printf("%s\t", row.get(i));
            }
            System.out.println();
        });
    }

    @Override
    public Iterator<DataRow> iterator() {
        int count = getPartitionCount();
        Iterator<DataRow>[] iters = new Iterator[count];
        for (int i=0; i<count; i++) {
            iters[i] = getPartition(i);
        }
        return new SequantialIterator(iters);
    }

    abstract int getPartitionCount();

    abstract Iterator<DataRow> getPartition(int index);


    public static <T> DataFrame from(Iterable<T> source, Class<T> clazz) {
        return new SourceDataFrame(new SimpleDataSource(source), clazz);
    }

    public static <T> DataFrame from(DataSource<T> source, Class<T> clazz) {
        return new SourceDataFrame(source, clazz);
    }

    public static <T> DataFrame from(
            DataSource<T> dataSource,
            DataColumn[] columns,
            BiFunction<T, String, Object> mapper) {
        return new SourceDataFrame(dataSource, columns, mapper);
    }


}
