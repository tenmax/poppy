package io.tenmax.poppy.dataframes;

import io.tenmax.poppy.DataColumn;
import io.tenmax.poppy.DataRow;
import io.tenmax.poppy.ProjectColumnSpec;

import java.util.HashMap;
import java.util.Iterator;
import java.util.function.Function;

public class ProjectDataFrame extends BaseDataFrame {
    private final ProjectColumnSpec[] specs;
    private final BaseDataFrame parent;
    private final HashMap<String, ProjectColumnSpec> specsMap= new HashMap<>();

    public ProjectDataFrame(BaseDataFrame parent, String[] columnNames) {
        this(parent, specsFromColumnNames(parent, columnNames));
    }

    public ProjectDataFrame(BaseDataFrame parent, ProjectColumnSpec[] specs) {
        super(parent.context, columnsFromSpec(fixSpecs(parent, specs)));
        this.parent = parent;
        this.specs = fixSpecs(parent, specs);

        for (ProjectColumnSpec spec: specs) {
            specsMap.put(spec.getColumn(), spec);
        }
    }

    private static ProjectColumnSpec[] fixSpecs(BaseDataFrame parent, ProjectColumnSpec[] specs) {
        ProjectColumnSpec[] newSpecs = new ProjectColumnSpec[specs.length];
        int i = 0;

        for (ProjectColumnSpec spec : specs) {
            String column = spec.getColumn();
            Class type = spec.getType();
            Function<DataRow, ?> mapper = spec.getMapper();
            if (type == null) {
                if (mapper == null) {
                    type = parent.getColumn(column).getType();
                } else {
                    new IllegalArgumentException("not type defined for " + column);
                }
            }
            if (mapper == null) {
                mapper = (DataRow row) -> row.get(column);
            }

            newSpecs[i++] = new ProjectColumnSpec(column, type, mapper);
        }

        return  newSpecs;
    }

    private static ProjectColumnSpec[] specsFromColumnNames(BaseDataFrame parent, String[] columnNames) {
        ProjectColumnSpec[] specs = new ProjectColumnSpec[columnNames.length];


        int i = 0;

        for (String columnName : columnNames) {

            specs[i++] = new ProjectColumnSpec(
                    columnName,
                    columnName,
                    parent.getColumn(columnName).getType(),
                    null);
        }

        return specs;
    }



    private static DataColumn[] columnsFromSpec(ProjectColumnSpec[] specs) {
        DataColumn[] dataColumns = new DataColumn[specs.length];
        int i = 0;
        for (ProjectColumnSpec spec : specs) {
            dataColumns[i++] = new DataColumn(spec.getColumn(), spec.getType());
        }
        return  dataColumns;
    }

    @Override
    int getPartitionCount() {
        return parent.getPartitionCount();
    }

    @Override
    Iterator<DataRow> getPartition(int index) {
        return new ProjectIterator(parent.getPartition(index));
    }

    class ProjectIterator implements Iterator<DataRow> {
        private Iterator<DataRow> wrapped;

        ProjectIterator(Iterator<DataRow> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public boolean hasNext() {
            return wrapped.hasNext();
        }

        @Override
        public DataRow next() {
            return new ProjectDataRow(wrapped.next());
        }
    }

    class ProjectDataRow implements DataRow {
        private DataRow row;

        ProjectDataRow(DataRow row) {
            this.row = row;
        }

        @Override
        public Object get(int index) {
            ProjectColumnSpec spec = specs[index];
            if (spec.getMapper() != null) {
                return spec.getMapper().apply(row);
            } else {
                return row.get(spec.getColumn());
            }
        }

        @Override
        public Object get(String name) {
            ProjectColumnSpec spec = specsMap.get(name);
            if (spec.getMapper() != null) {
                return spec.getMapper().apply(row);
            } else {
                return row.get(spec.getColumn());
            }
        }
    }
}
