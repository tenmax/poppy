package io.tenmax.poppy.dataframes;

import io.tenmax.poppy.AggregateColumnSpec;
import io.tenmax.poppy.DataColumn;
import io.tenmax.poppy.DataRow;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class AggregateDataFrame extends  BaseDataFrame{
    private final AggregateColumnSpec[] specs;
    private final BaseDataFrame parent;
    private final HashMap<String, AggregateColumnSpec> specsMap= new HashMap<>();
    private final int dimSize;

    public AggregateDataFrame(BaseDataFrame parent, AggregateColumnSpec[] specs) {
        super(new ExecutionContext(), columnsFromSpec(parent, specs));
        this.parent = parent;
        this.specs = specs;
        this.dimSize = parent.groupedColumns.length;
        this.groupedColumns = parent.groupedColumns;

        for (AggregateColumnSpec spec: specs) {
            specsMap.put(spec.getColumn(), spec);
        }
    }

    private static DataColumn[] columnsFromSpec(BaseDataFrame parent, AggregateColumnSpec[] specs) {
        DataColumn[] dataColumns = new DataColumn[parent.groupedColumns.length + specs.length];
        int i = 0;

        for (DataColumn dataColumn : parent.groupedColumns) {
            dataColumns[i++] = new DataColumn(dataColumn.getName(), dataColumn.getType());
        }

        for (AggregateColumnSpec spec : specs) {
            dataColumns[i++] = new DataColumn(spec.getColumn(), spec.getType());
        }

        return  dataColumns;
    }

    @Override
    public int getPartitionCount() {
        return 1;
    }

    @Override
    public Iterator<DataRow> getPartition(int index) {
        int count = parent.getPartitionCount();
        HashMap<List, List> result = new HashMap<>();


        if (parent.context.getNumThreads() == 1) {
            // sequatial
            for (int i = 0; i < count; i++) {
                HashMap<List, List> resultPartial = accumulate(parent.getPartition(i));
                combine(result, resultPartial);
            }
        } else {
            // parallel
            ExecutorService executorService = Executors.newFixedThreadPool(parent.context.getNumThreads());
            ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();

            for (int i = 0; i < count; i++) {

                final int fi = i;

                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    HashMap<List, List> resultPartial = accumulate(parent.getPartition(fi));
                    synchronized (result) {
                        combine(result, resultPartial);
                    }
                }, executorService);

                futures.add(future);
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            // shutdown the executor while all tasks complete
            executorService.shutdown();
        }

        HashMap<List, List> finalResult = finish(result);
        return new AggregateIterator(finalResult);
    }


    HashMap<List, List> accumulate(Iterator<DataRow> iterator) {
        HashMap<List, List> result = new HashMap<>();

        while(iterator.hasNext()) {
            DataRow row = iterator.next();

            List dims  = new ArrayList<>();
            for (DataColumn gc : parent.groupedColumns) {
                dims.add(row.get(gc.getName()));
            }

            List accus;          // accumulators

            if (result.containsKey(dims)) {
                accus = result.get(dims);
            } else {
                accus = new ArrayList(specs.length);
                for (int i = 0; i < specs.length; i++) {
                    accus.add(specs[i].getCollector().supplier().get());
                }
                result.put(dims, accus);
            }

            // aggregate
            for (int i=0; i<specs.length; i++) {
                specs[i].getCollector().accumulator().accept(accus.get(i), row);
            }
        }

        return result;
    }

    void combine(HashMap<List, List> result1, HashMap<List, List> result2) {

        result2.forEach((dims, accus2) -> {
            if (result1.containsKey(dims)) {
                List accus1 = result1.get(dims);
                for (int i=0; i<specs.length; i++) {
                    Object accu = specs[i].getCollector().combiner().apply(accus1.get(i), accus2.get(i));
                    accus1.set(i, accu);
                }
            } else {
                result1.put(dims,accus2);
            }
        });
    }

    HashMap<List, List> finish(HashMap<List, List> result) {
        final HashMap<List, List> finalResult = new HashMap<>();

        result.forEach((dims, accus) -> {

            List values = new ArrayList(specs.length);
            for (int i=0; i<specs.length; i++) {
                Function finisher = specs[i].getCollector().finisher();
                if (finisher != null) {
                    values.add(finisher.apply(accus.get(i)));
                } else {
                    values.add(accus.get(i));
                }
            }
            finalResult.put(dims,values);
        });

        return finalResult;
    }

    class AggregateIterator implements Iterator<DataRow> {
        private Iterator<Map.Entry<List, List>> iterator;

        AggregateIterator(HashMap<List, List> result) {
            this.iterator = result.entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public DataRow next() {
            return new AggregateDataRow(iterator.next());
        }
    }

    class AggregateDataRow extends BaseDataRow{
        private Map.Entry<List, List> entry;

        AggregateDataRow(Map.Entry<List, List> entry) {
            this.entry = entry;
        }

        @Override
        public Object get(int index) {
            if (index < dimSize) {
                return entry.getKey().get(index);
            } else {
                return entry.getValue().get(index - dimSize);
            }
        }
    }
}
