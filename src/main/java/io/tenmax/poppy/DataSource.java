package io.tenmax.poppy;

import java.util.Iterator;

public interface DataSource<T> {

    int getPartitionCount();

    Iterator<T> getPartition(int index);
}
