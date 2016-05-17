package io.tenmax.poppy.dataframes;

import io.tenmax.poppy.*;
import io.tenmax.poppy.datasources.SimpleDataSource;
import io.tenmax.poppy.exceptions.ColumnNotFoundException;
import io.tenmax.poppy.exceptions.ReflectionException;
import io.tenmax.poppy.iterators.ParallelIterator;
import io.tenmax.poppy.iterators.SequantialIterator;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

abstract public class BaseDataFrame implements DataFrame{
    protected final ExecutionContext context;
    protected final DataColumn[] columns;
    protected final HashMap<String, Integer> columnsMap;
    protected DataColumn[] groupedColumns = new DataColumn[0];

    public BaseDataFrame(ExecutionContext context, DataColumn[] columns) {
        this.context = context;
        this.columns = columns;
        this.columnsMap = new HashMap<>();

        int i = 0;
        for (DataColumn column: columns) {
            columnsMap.put(column.getName(), i++);
        }
    }

    @Override
    public DataColumn[] getColumns() {
        return columns;
    }

    @Override
    public DataColumn getColumn(String name) {
        Integer index = columnsMap.get(name);
        if(index == null) {
            throw new ColumnNotFoundException(name);
        }

        return columns[index];
    }

    @Override
    public DataColumn getColumn(int index) {
        return columns[index];
    }

    @Override
    public DataFrame project(String... columns) {
        return new ProjectDataFrame(this, columns);
    }

    @Override
    public DataFrame project(ProjectColumnSpec... columns) {
        return new ProjectDataFrame(this, columns);
    }

    @Override
    public DataFrame groupby(String... groupedColumns) {
        DataColumn[] gc = new DataColumn[groupedColumns.length];

        for (int i=0; i<groupedColumns.length; i++) {
            gc[i] = getColumn(groupedColumns[i]);
        }
        this.groupedColumns = gc;

        return this;
    }

    @Override
    public DataFrame aggregate(AggregateColumnSpec... specs) {
        return new AggregateDataFrame(this, specs);
    }

    @Override
    public DataFrame sort(String... columns) {
        SortSpec[] specs = new SortSpec[columns.length];
        for (int i=0; i<columns.length; i++) {
            specs[i] = new SortSpec(columns[i], SortSpec.Order.ASC);
        }
        return sort(specs);
    }

    @Override
    public DataFrame sort(SortSpec... specs) {
        return new SortDataFrame(this, specs);
    }

    @Override
    public DataFrame distinct(String... columns) {
        return new DistinctDataFrame(this, columns);
    }

    @Override
    public DataFrame filter(Predicate<DataRow> predicate) {
        return new FilterDataFrame(this, predicate);
    }

    @Override
    public DataFrame peek(Consumer<DataRow> consumer) {
        return new PeekDataFrame(this, consumer);
    }

    @Override
    public DataFrame parallel(int numThreads) {
        context.setNumThreads(numThreads);
        return this;
    }

    @Override
    public RandomAccessDataFrame cache() {
        return new CacheDataFrame(this);
    }

    @Override
    public void print() {
        Arrays.stream(columns).forEach(column ->{
            System.out.printf("%s\t", column.getName());
        });
        System.out.println();

        forEach((row) -> {
            for (Object o : row) {
                System.out.printf("%s\t", o);
            }

            System.out.println();
        });
    }

    @Override
    public Iterator<DataRow> iterator() {


        if (context.getNumThreads() > 1) {
            return new ParallelIterator(this);
        } else {
            return new SequantialIterator(this);
        }
    }

    public void forEachPartition(BiConsumer<Integer, DataRow> consumer) {
        int partitionCount = getPartitionCount();

        if (partitionCount == 1) {
            for (int i = 0; i < partitionCount; i++) {
                Iterator<DataRow> partition = getPartition(i);
                while (partition.hasNext()) {
                    consumer.accept(i, partition.next());
                }
            }
        } else {
            forEachPartitionAsync(consumer).join();
        }
    }

    public CompletableFuture<Void> forEachPartitionAsync(BiConsumer<Integer, DataRow> consumer) {
        ExecutorService executorService = Executors.newFixedThreadPool(context.getNumThreads());
        int partitionCount = getPartitionCount();
        CompletableFuture[] futures = new CompletableFuture[partitionCount];

        for (int i=0; i<partitionCount; i++) {
            final int fi = i;
            futures[i] =
            CompletableFuture.runAsync(() -> {
                Iterator<DataRow> partition = getPartition(fi);
                while (partition.hasNext()) {
                    consumer.accept(fi, partition.next());
                }
            }, executorService);
        }

        CompletableFuture<Void> future = CompletableFuture.allOf(futures);
        executorService.shutdown();

        return future;
    }

    @Override
    public void to(DataSink sink) {
        sink.sinkStart(getPartitionCount(), columns);

        ExecutorService executorService = Executors.newFixedThreadPool(context.getNumThreads());
        int partitionCount = getPartitionCount();
        CompletableFuture[] futures = new CompletableFuture[partitionCount];

        AtomicInteger counter = new AtomicInteger();
        for (int i=0; i<partitionCount; i++) {
            final int fi = i;
            futures[i] =
            CompletableFuture.runAsync(() -> {
                sink.partitionStart(fi);

                Iterator<DataRow> partition = getPartition(fi);
                while (partition.hasNext()) {
                    sink.partitionRow(fi, partition.next());
                }

                sink.partitionComplete(fi);

            }, executorService);
        }

        CompletableFuture<Void> future = CompletableFuture.allOf(futures);
        executorService.shutdown();
        future.join();

        sink.sinkComplete();
    }

    @Override
    public List<List> toList() {
        ArrayList<List> list = new ArrayList<>();

        for (DataRow row : this) {
            List data = new ArrayList();
            for (int i=0; i<columns.length; i++) {
                data.add(row.get(i));
            }
            list.add(data);
        }

        return list;
    }

    @Override
    public <T> List<T> toList(Class<T> clazz) {
        ArrayList<T> list = new ArrayList<>();

        try {
            for (DataRow row : this) {
                T t = clazz.newInstance();

                for (DataColumn column : columns) {
                    if (PropertyUtils.isWriteable(t, column.getName())) {
                        PropertyUtils.setProperty(t, column.getName(), row.get(column.getName()));
                    }
                }
                list.add(t);
            }
        } catch (InstantiationException |
                IllegalAccessException |
                NoSuchMethodException  |
                InvocationTargetException e)
        {
            throw new ReflectionException(e);
        }

        return list;
    }

    @Override
    public Map<List, List> toMap() {
        HashMap<List, List> map = new HashMap<>();

        List<Integer> keyColumns = new ArrayList<>();
        List<Integer> valueColumns = new ArrayList<>();

        Set<String> groupedColumnsSet = new HashSet();

        for (DataColumn groupColumn : groupedColumns) {
            groupedColumnsSet.add(groupColumn.getName());
        }

        for (int i=0; i<columns.length; i++) {
            if (groupedColumnsSet.contains(columns[i].getName())) {
                keyColumns.add(i);
            } else {
                valueColumns.add(i);
            }
        }

        for (DataRow row : this) {

            List key = new ArrayList();
            List value = new ArrayList();

            for (int i: keyColumns) {
                key.add(row.get(i));
            }

            for (int i: valueColumns) {
                value.add(row.get(i));
            }

            map.put(key, value);
        }

        return map;
    }

    @Override
    public <K, V> Map<K, V> toMap(Class<K> keyClazz, Class<V> valueClazz) {
        HashMap<K, V> map = new HashMap<>();

        List<Integer> keyColumns = new ArrayList<>();

        Set<String> groupedColumnsSet = new HashSet();

        for (DataColumn groupColumn : groupedColumns) {
            groupedColumnsSet.add(groupColumn.getName());
        }

        for (int i=0; i<columns.length; i++) {
            if (groupedColumnsSet.contains(columns[i].getName())) {
                keyColumns.add(i);
            }
        }

        PropertyDescriptor[] props;

        HashSet<String> keyProps = new HashSet<>();
        props = PropertyUtils.getPropertyDescriptors(keyClazz);
        for (PropertyDescriptor prop : props) {
            if (prop.getWriteMethod() != null) {
                keyProps.add(prop.getName());
            }
        }

        HashSet<String> valueProps = new HashSet<>();
        props = PropertyUtils.getPropertyDescriptors(valueClazz);
        for (PropertyDescriptor prop : props) {
            if (prop.getWriteMethod() != null) {
                valueProps.add(prop.getName());
            }
        }


        try {
            for (DataRow row : this) {
                K key = keyClazz.newInstance();
                V value = valueClazz.newInstance();


                for (int i: keyColumns) {
                    String columnName = columns[i].getName();

                    if (keyProps.contains(columnName)) {
                        PropertyUtils.setProperty(
                                key,
                                columnName,
                                row.get(columnName)
                        );
                    }
                }

                for (DataColumn column: columns) {
                    String columnName = column.getName();

                    if (valueProps.contains(columnName)) {
                        PropertyUtils.setProperty(
                                value,
                                columnName,
                                row.get(columnName)
                        );
                    }
                }

                map.put(key, value);
            }
        } catch (InstantiationException |
                IllegalAccessException |
                NoSuchMethodException  |
                InvocationTargetException e)
        {
            throw new ReflectionException(e);
        }

        return map;
    }

    abstract public int getPartitionCount();

    public ExecutionContext getContext() {
        return context;
    }

    abstract public Iterator<DataRow> getPartition(int index);

    public static <T> DataFrame from(Iterable<T> source, Class<T> clazz) {
        return new SourceDataFrame(new SimpleDataSource(clazz, source));
    }

    public static <T> DataFrame from(DataSource<T> source) {
        return new SourceDataFrame(source);
    }


    abstract class BaseDataRow implements DataRow {
        @Override
        public DataColumn[] getColumns() {
            return columns;
        }

        @Override
        public Object get(String name) {
            return get(columnsMap.get(name));
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            for (int i=0; i<columns.length; i++) {
                sb.append(columns[i].getName() + "=" + get(i) + ",");
            }

            return sb.toString();
        }
    }
}
