package io.tenmax.poppy;

import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class SpecUtils {
    // ProjectColumnSpec

    public static ProjectColumnSpec col(String columnName) {
        return new ProjectColumnSpecBuilder().as(columnName);
    }

    public static ProjectColumnSpecBuilder colMap(String columnRef) {
        return new ProjectColumnSpecBuilder<Object>(null, (DataRow row) -> row.get(columnRef));
    }

    public static <T,R> ProjectColumnSpecBuilder colMap(String columnRef, Class<T> type, Function<R, T> mapper) {
        return new ProjectColumnSpecBuilder<T>(type, (DataRow row) -> mapper.apply((R)row.get(columnRef)));
    }

    public static class ProjectColumnSpecBuilder<T> {
        private final Class<T> type;
        private final Function<DataRow, T> mapper;

        public ProjectColumnSpecBuilder() {
            this(null, null);
        }

        public ProjectColumnSpecBuilder(Class<T> type, Function<DataRow, T> mapper) {
            this.type = type;
            this.mapper = mapper;
        }

        public ProjectColumnSpec<T> as(String column) {
            return new ProjectColumnSpec<>(column, type, mapper);
        }
    }

    // AggregateColumnSpec
    public static AggregateColumnSpecBuilder sumLong(String columnRef) {
        return new AggregateColumnSpecBuilder(
                Long.class,
                Collectors.summingLong((DataRow row) -> row.getLong(columnRef)));
    }

    public static AggregateColumnSpecBuilder sumDouble(String columnRef) {
        return new AggregateColumnSpecBuilder(
                Double.class,
                Collectors.summingDouble((DataRow row) -> row.getDouble(columnRef)));
    }

    public static AggregateColumnSpecBuilder avgLong(String columnRef) {
        return new AggregateColumnSpecBuilder(
                Double.class,
                Collectors.averagingLong((DataRow row) -> row.getLong(columnRef)));
    }

    public static AggregateColumnSpecBuilder avgDouble(String columnRef) {
        return new AggregateColumnSpecBuilder(
                Double.class,
                Collectors.averagingDouble((DataRow row) -> row.getDouble(columnRef)));
    }

    public static AggregateColumnSpecBuilder count() {
        return new AggregateColumnSpecBuilder(
                Long.class,
                Collectors.counting());
    }

    public static AggregateColumnSpecBuilder count(String columnRef) {
        return new AggregateColumnSpecBuilder(
                Long.class,
                Collectors.summingLong((DataRow row) -> row.get(columnRef) != null ? 1 : 0));
    }

    public static AggregateColumnSpecBuilder min(String columnRef) {
        Function<DataRow, ?> mapper = row -> row.get(columnRef);
        Comparator comparator = Comparator.naturalOrder();
        Collector<?,?,Optional<?>> collector = Collectors
                .<DataRow,Object,Object,Object>mapping(mapper, Collectors.minBy(comparator));
        Collector collector2 = Collectors.collectingAndThen(collector, (opt) -> opt.orElse(null));

        return new AggregateColumnSpecBuilder(columnRef,collector2);
    }

    public static AggregateColumnSpecBuilder max(String columnRef) {
        Function<DataRow, ?> mapper = row -> row.get(columnRef);
        Comparator comparator = Comparator.naturalOrder();
        Collector<?,?,Optional<?>> collector = Collectors
                .<DataRow,Object,Object,Object>mapping(mapper, Collectors.maxBy(comparator));
        Collector collector2 = Collectors.collectingAndThen(collector, (opt) -> opt.orElse(null));

        return new AggregateColumnSpecBuilder(columnRef,collector2);

    }

    public static <T, A, R> AggregateColumnSpecBuilder aggreMap(String columnRef, Class<R> type, Collector<T,A,R> collector) {
        Function<DataRow, T> mapper = row -> (T)row.get(columnRef);
        Collector<DataRow, ?, R> newCollector = Collectors.mapping(mapper, collector);
        return new AggregateColumnSpecBuilder(type,newCollector);
    }


    public static class AggregateColumnSpecBuilder<T> {
        private Class<T> type;
        private String typeFromColumn;
        private Collector<DataRow, ?, T> collector;

        public AggregateColumnSpecBuilder(Class<T> type, Collector<DataRow, ?, T> collector) {
            this.type = type;
            this.collector = collector;
        }

        public AggregateColumnSpecBuilder(String typeFromColumn, Collector<DataRow, ?, T> collector) {
            this.typeFromColumn = typeFromColumn;
            this.collector = collector;
        }

        public AggregateColumnSpec<T> as(String column) {
            if (type != null) {
                return new AggregateColumnSpec<>(column, type, collector);
            }

            if (typeFromColumn != null) {
                return new AggregateColumnSpec<T>(column, typeFromColumn, collector);
            }

            throw new IllegalStateException("type and typeFromColumn not defined");
        }
    }

    // SortSpec
    public static SortSpec asc(String name) {
        return new SortSpec(name, SortSpec.Order.ASC);
    }

    public static SortSpec desc(String name) {
        return new SortSpec(name, SortSpec.Order.DESC);
    }


}
