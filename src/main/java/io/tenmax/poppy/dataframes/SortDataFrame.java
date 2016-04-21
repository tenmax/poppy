package io.tenmax.poppy.dataframes;

import io.tenmax.poppy.DataColumn;
import io.tenmax.poppy.DataRow;
import io.tenmax.poppy.SortSpec;
import io.tenmax.poppy.exceptions.ColumnNotSortableException;

import java.util.*;

public class SortDataFrame extends BaseDataFrame{

    private final SortSpec[] specs;
    private final BaseDataFrame parent;

    public SortDataFrame(BaseDataFrame parent, SortSpec[] specs) {
        super(parent.columns);

        for (SortSpec spec: specs) {
            DataColumn column = parent.getColumn(spec.getColumn());
            if (column.getType().isAssignableFrom(Comparable.class)) {
                throw new ColumnNotSortableException(spec.getColumn());
            }
        }

        this.parent = parent;
        this.specs = specs;

    }

    @Override
    int getPartitionCount() {
        return 1;
    }

    @Override
    Iterator<DataRow> getPartition(int index) {
        int count = parent.getPartitionCount();
        ArrayList<DataRow> rows = new ArrayList<>();

        for (int i=0; i<count; i++) {
            Iterator<DataRow> partition = parent.getPartition(i);
            while (partition.hasNext()) {
                rows.add(partition.next());
            }
        }

        Collections.sort(rows, new DataRowComparator());
        return rows.iterator();
    }

    class DataRowComparator implements Comparator<DataRow> {
        @Override
        public int compare(DataRow row1, DataRow row2) {
            for (SortSpec spec: specs) {
                Comparable v1 = (Comparable)row1.get(spec.getColumn());
                Comparable v2 = (Comparable)row2.get(spec.getColumn());

                if (v1.compareTo(v2) == 0) {
                    continue;
                }

                return spec.getOrder() == SortSpec.Order.ASC ?
                       v1.compareTo(v2) :
                       -v1.compareTo(v2);
            }

            return 0;
        }
    }
}
