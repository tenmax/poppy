package io.tenmax.poppy;

import java.util.Iterator;

public class SimpleDataSource<T> implements DataSource<T> {
    private Iterable<T>[] iterables;

    public SimpleDataSource(Iterable<T>... iterables) {
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
