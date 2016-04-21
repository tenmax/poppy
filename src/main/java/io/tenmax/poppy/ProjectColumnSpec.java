package io.tenmax.poppy;

import java.util.function.Function;

public class ProjectColumnSpec<T> {
    private final String column;
    private final Class<T> type;
    private final Function<DataRow, T> mapper;

    public ProjectColumnSpec(String column, Class<T> type, Function<DataRow, T> mapper) {
        this.column = column;
        this.type = type;
        this.mapper = mapper;
    }

    public ProjectColumnSpec(String column, String from, Class<T> type, Function<? super Object, T> mapper) {
        this.column = column;
        this.type = type;

        if (mapper == null) {
            this.mapper = (row) -> (T)row.get(from);
        } else {
            this.mapper = (row) -> mapper.apply(row.get(from));
        }
    }

    public String getColumn() {
        return column;
    }

    public Class<T> getType() {
        return type;
    }

    public Function<DataRow, T> getMapper() {
        return mapper;
    }
}
