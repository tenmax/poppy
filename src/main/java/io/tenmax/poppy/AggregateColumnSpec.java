package io.tenmax.poppy;

import java.util.stream.Collector;

public class AggregateColumnSpec<T> {
    private final String column;
    private final Class<T> type;
    private final Collector<DataRow, ?, T> collector;

    public AggregateColumnSpec(String column, Class<T> type, Collector<DataRow, ?, T> collector) {
        this.column = column;
        this.type = type;
        this.collector = collector;
    }

    public String getColumn() {
        return column;
    }

    public Class<T> getType() {
        return type;
    }

    public Collector<DataRow, ?, T> getCollector() {
        return collector;
    }
}

