package io.tenmax.poppy.datasinks;

import io.tenmax.poppy.DataColumn;
import io.tenmax.poppy.DataRow;
import io.tenmax.poppy.DataSink;

public class DebugDataSink implements DataSink{
    @Override
    public void sinkStart(int partitionCount, DataColumn[] columns) {
        System.out.printf("[sinkStart] partitionCount=%d\n", partitionCount);
        for (DataColumn column : columns) {
            System.out.printf("\t%s\t%s\n", column.getType().getName(),column.getName());
        }
    }

    @Override
    public void sinkComplete() {
        System.out.printf("[sinkComplete]\n");
    }

    @Override
    public void partitionStart(int partiton) {
        System.out.printf("[partitionStart] partition=%d\n", partiton);
    }

    @Override
    public void partitionRow(int partition, DataRow row) {
        System.out.printf("[partitionRow] partition=%d\n", partition);
        System.out.printf("    %s\n", row);
    }

    @Override
    public void partitionComplete(int partiton) {
        System.out.printf("[partitionComplete] partition=%d\n", partiton);
    }
}
