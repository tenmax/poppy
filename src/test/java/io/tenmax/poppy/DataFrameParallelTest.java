package io.tenmax.poppy;

import io.tenmax.poppy.datasinks.DebugDataSink;
import io.tenmax.poppy.datasources.SimpleDataSource;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.tenmax.poppy.SpecUtils.*;
import static io.tenmax.poppy.SpecUtils.desc;

public class DataFrameParallelTest extends TestCase {

    private DataFrame df;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        ArrayList<Student> list1 = new ArrayList<>();
        ArrayList<Student> list2 = new ArrayList<>();
        ArrayList<Student> list3 = new ArrayList<>();

        list1.add(new Student(1, "pop",     5,2,170,60));
        list1.add(new Student(2, "foo",     5,3,175,70));
        list1.add(new Student(3, "bar",     5,4,168,80));
        list1.add(new Student(4, "john",    5,4,160,60));


        list2.add(new Student(5, "richard", 4,1,170,68));
        list2.add(new Student(6, "howard",  4,2,178,90));
        list2.add(new Student(7, "michael", 4,3,169,80));
        list2.add(new Student(8, "coco",    4,4,158,65));


        list3.add(new Student(9, "tina",    3,2,155,44));
        list3.add(new Student(10, "chloe",  3,2,158,45));
        list3.add(new Student(11, "george", 3,5,163,90));
        list3.add(new Student(12, "mary",   3,1,170,60));

        df= DataFrame.from(
            new SimpleDataSource<>(Student.class,list1, list2, list3))
            .parallel(4);
    }

    public void testBasic() throws Exception {
        df
        .print();
    }

    public void testProject() throws Exception {
        df
        .project("name", "weight", "height")
        .print();
    }

    public void testProject2() throws Exception {
        df
        .project(
                col("name"),
                colMap("weight").as("w"),
                colMap("height", Float.class, (Integer height) -> (height / 10f)).as("h"))
        .print();
    }

    public void testFilter() throws Exception {
        df
        .filter(row -> row.getInteger("height") >= 170)
        .project("name", "weight", "height")
        .print();
    }

    public void testAggre() throws Exception {
        df
        .aggregate(
                avgLong("weight").as("weight"),
                avgLong("height").as("height"),
                count().as("count"),
                aggreMap("weight", Integer.class, Collectors.summingInt((Integer i) -> i)).as("wi"))
        .print();
    }

    public void testGroupBy() throws Exception {
        df
        .groupby("grade", "room")
        .aggregate(
                avgLong("weight").as("weight"),
                avgLong("height").as("height"))
        .sort("grade", "room")
        .print();
    }

    public void testSort() throws Exception {
        df
        .sort("weight", "height")
        .print();
    }

    public void testSort2() throws Exception {
        df
        .sort(asc("weight"), desc("height"))
        .print();
    }

    public void testDistinct() throws Exception {
        df
        .distinct("grade", "room")
        .print();
    }

    public void testTo() throws Exception {
        TestDataSink sink = new TestDataSink();
//        df.to(new DebugDataSink());
        df.to(sink);
        assertEquals(1, sink.sinkStart.get());
        assertEquals(1, sink.sinkComplete.get());
        assertEquals(3, sink.partitionStart.get());
        assertEquals(12, sink.partitionRow.get());
        assertEquals(3, sink.partitionComplete.get());
    }

    class TestDataSink implements DataSink {
        AtomicInteger sinkStart = new AtomicInteger();
        AtomicInteger sinkComplete = new AtomicInteger();
        AtomicInteger partitionStart = new AtomicInteger();
        AtomicInteger partitionRow = new AtomicInteger();
        AtomicInteger partitionComplete = new AtomicInteger();

        @Override
        public void sinkStart(int partitionCount, DataColumn[] columns) {
            sinkStart.incrementAndGet();
        }

        @Override
        public void sinkComplete() {
            sinkComplete.incrementAndGet();

        }

        @Override
        public void partitionStart(int partition) {
            partitionStart.incrementAndGet();
        }

        @Override
        public void partitionRow(int partition, DataRow row) {
            partitionRow.incrementAndGet();
        }

        @Override
        public void partitionComplete(int partition) {
            partitionComplete.incrementAndGet();
        }
    }
}
