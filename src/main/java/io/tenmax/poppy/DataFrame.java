package io.tenmax.poppy;

import io.tenmax.poppy.dataframes.BaseDataFrame;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Consumer;

/**
 * DataFrame is a sequence of schema-defined rows. The following
 * example illustrates how to use the {@link DataFrame}:
 *
 * <pre>{@code
 * List<Student> students = ...;
 *
 * DataFrame.from(students, Student.class)
 *          .groupby("grade", "room")
 *          .aggregate(
 *              avgLong("weight").as("weight"),
 *              avgLong("height").as("height"))
 *          .sort("grade", "room")
 *          .print();
 * }</pre>
 *
 * Just like {@link java.util.stream.Stream}, it iterates through a
 * data source with as little memory as possible. This allows you processing
 * billion of data with only constant memory.
 *
 * DataFrame provides operations which SQL provides. For example {@link #project(ProjectColumnSpec[]) projection},
 * ,{@link #filter(java.util.function.Predicate) filtering}, {@link #groupby(String...) grouping}, and
 * {@link #aggregate(AggregateColumnSpec[]) aggregation}, {@link #sort(SortSpec...) soring}. These operations make it possible to
 * write your own SQL-like statements in your application. In the above example, it is equivalent to
 *
 * <pre>{@code
 *     select
 *         grade,
 *         room,
 *         avg(weight) as weight,
 *         avg(height) as height
 *     from Student
 *     group by grade, room
 *     order by grade, room
 * }</pre>
 *
 */
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
