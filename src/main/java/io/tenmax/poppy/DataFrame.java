package io.tenmax.poppy;

import io.tenmax.poppy.dataframes.BaseDataFrame;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public interface DataFrame extends Iterable<DataRow>{

    DataColumn[] getColumns();

    DataColumn getColumn(String name);

    DataColumn getColumn(int index);

    static <T> DataFrame from(Iterable<T> source, Class<T> clazz) {
        return BaseDataFrame.from(source,clazz);
    }

    static <T> DataFrame from(DataSource<T> source, Class<T> clazz) {
        return BaseDataFrame.from(source,clazz);
    }

    static <T> DataFrame from(
            DataSource<T> source,
            DataColumn[] columns,
            BiFunction<T, String, Object> mapper) {
        return BaseDataFrame.from(source, columns, mapper);
    }

    DataFrame project(String... columns);

    DataFrame project(ProjectColumnSpec... columns);

    DataFrame groupby(String... columns);

    DataFrame aggregate(AggregateColumnSpec... specs);

    DataFrame sort(String... columns);

    DataFrame sort(SortSpec... columns);

    DataFrame distinct(String... columns);

    DataFrame filter(Predicate<DataRow> predicate);

    void print();
}
