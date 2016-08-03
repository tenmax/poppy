package io.tenmax.poppy.datasources;

import java.util.Iterator;

/**
 * SimpleDataSource use the java reflection to define the columns. And using
 * the Java Bean conversion to get the value of a column.
 *
 * @param <T> The source data type.
 */
public class SimpleDataSource<T> extends ReflectionDataSource<T> {
    private final Iterable<T>[] iterables;

    public SimpleDataSource(Class<T> clazz, Iterable<T>... iterables) {
        super(clazz);
        this.iterables = iterables;
    }

    @Override
    public int getPartitionCount() {
        return iterables.length;
    }

    @Override
    public Iterator<T> getPartition(int index) {
        return iterables[index].iterator();
    }
}
