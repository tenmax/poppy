package io.tenmax.poppy;

import io.tenmax.poppy.dataframes.BaseDataFrame;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Consumer;

public interface DataFrame extends Iterable<DataRow>{

    DataColumn[] getColumns();

    DataColumn getColumn(String name);

    DataColumn getColumn(int index);

    static <T> DataFrame from(Iterable<T> source, Class<T> clazz) {
        return BaseDataFrame.from(source,clazz);
    }

    static <T> DataFrame from(DataSource<T> source) {
        return BaseDataFrame.from(source);
    }


    DataFrame project(String... columns);

    DataFrame project(ProjectColumnSpec... columns);

    DataFrame groupby(String... columns);

    DataFrame aggregate(AggregateColumnSpec... specs);

    DataFrame sort(String... columns);

    DataFrame sort(SortSpec... columns);

    DataFrame distinct(String... columns);

    DataFrame peek(Consumer<DataRow> consumer);

    DataFrame filter(Predicate<DataRow> predicate);

    DataFrame parallel(int numThreads);

    RandomAccessDataFrame cache();

    void to(DataSink sink);

    List<List> toList();

    <T> List<T> toList(Class<T> clazz);

    Map<List, List> toMap();

    <K, V> Map<K, V> toMap(Class<K> keyClazz, Class<V> valueClazz);

    void print();
}
