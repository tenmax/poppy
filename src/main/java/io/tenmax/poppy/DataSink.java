package io.tenmax.poppy;

public interface DataSink {

    default void sinkStart(int partitionCount, DataColumn[] columns){}

    default void sinkComplete(){}

    default void partitionStart(int partition){}

    default void partitionRow(int partition, DataRow row){}

    default void partitionComplete(int partition){}
}
