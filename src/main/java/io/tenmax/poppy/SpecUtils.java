package io.tenmax.poppy;

import java.util.Objects;
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

        ProjectColumnSpec<T> as(String column) {
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

    public static <T, A, R> AggregateColumnSpecBuilder aggreMap(String columnRef, Class<R> type, Collector<T,A,R> collector) {
        Collector<DataRow, A, R> newCollector = new Collector<DataRow, A, R>() {
            @Override
            public Supplier<A> supplier() {
                return collector.supplier();
            }

            @Override
            public BiConsumer<A, DataRow> accumulator() {
                return (a, row) -> {
                    T value = (T)row.get(columnRef);
                    collector.accumulator().accept(a, value);
                };
            }

            @Override
            public BinaryOperator<A> combiner() {
                return collector.combiner();
            }

            @Override
            public Function<A, R> finisher() {
                return collector.finisher();
            }

            @Override
            public Set<Characteristics> characteristics() {
                return collector.characteristics();
            }
        };

        return new AggregateColumnSpecBuilder(type,newCollector);
    }


    public static class AggregateColumnSpecBuilder<T> {
        private Class<T> type;
        private Collector<DataRow, ?, T> collector;

        public AggregateColumnSpecBuilder(Class<T> type, Collector<DataRow, ?, T> collector) {
            this.type = type;
            this.collector = collector;
        }

        AggregateColumnSpec<T> as(String column) {
            return new AggregateColumnSpec<>(column, type, collector);
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
