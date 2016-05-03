package io.tenmax.poppy;

import io.tenmax.poppy.datasinks.DebugDataSink;
import io.tenmax.poppy.datasources.SimpleDataSource;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.tenmax.poppy.SpecUtils.*;
import static io.tenmax.poppy.SpecUtils.desc;

public class DataFrameExceptionalTest {


    @Test
    (expected = RuntimeException.class)
    public void testAggre1() throws Exception {
        DataFrame.from(new ExceptionalDataSource(ExceptionalDataSource.ErrorType.GetPartitionCount))
        .parallel(4)
        .aggregate(
                avgLong("weight").as("weight"),
                avgLong("height").as("height"),
                count().as("count"),
                aggreMap("weight", Integer.class, Collectors.summingInt((Integer i) -> i)).as("wi"))
        .print();
    }

    @Test
    (expected = RuntimeException.class)
    public void testAggre2() throws Exception {
        DataFrame.from(new ExceptionalDataSource(ExceptionalDataSource.ErrorType.GetPartition))
        .parallel(4)
        .aggregate(
                avgLong("weight").as("weight"),
                avgLong("height").as("height"),
                count().as("count"),
                aggreMap("weight", Integer.class, Collectors.summingInt((Integer i) -> i)).as("wi"))
        .print();
    }

    @Test
            (expected = RuntimeException.class)
    public void testAggre3() throws Exception {
        DataFrame.from(new ExceptionalDataSource(ExceptionalDataSource.ErrorType.Iterator))
        .parallel(4)
        .aggregate(
                avgLong("weight").as("weight"),
                avgLong("height").as("height"),
                count().as("count"),
                aggreMap("weight", Integer.class, Collectors.summingInt((Integer i) -> i)).as("wi"))
        .print();
    }

}


class ExceptionalDataSource implements DataSource<Student> {
    enum ErrorType {
        GetPartitionCount,
        GetPartition,
        Iterator
    }

    private ErrorType errorType;

    ExceptionalDataSource(ErrorType errorType) {
        this.errorType = errorType;
    }

    @Override
    public int getPartitionCount() {
        if(errorType == ErrorType.GetPartitionCount) {
            throw new RuntimeException("hello exception");
        }

        return 3;
    }

    @Override
    public Iterator<Student> getPartition(int index) {

        if (index > 0) {

            if (errorType == ErrorType.GetPartition) {
                throw new RuntimeException("hello exception");
            } else if(errorType == ErrorType.Iterator) {
                return new ExceptionalIterator();
            }
        }

        return Arrays.asList(new Student(1, "pop", 5, 2, 176, 68)).iterator();
    }

    @Override
    public DataColumn[] getColumns() {
        return new DataColumn[] {
            new DataColumn("name", String.class),
            new DataColumn("weight", Integer.class),
            new DataColumn("height", Integer.class)
        };
    }

    @Override
    public Object get(Student student, String columnName) {
        switch (columnName) {
            case "name":
                return student.getName();
            case "weight":
                return student.getWeight();
            case "height":
                return student.getHeight();
        }
        return null;
    }
}

class ExceptionalIterator implements Iterator<Student> {
    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Student next() {
        throw new RuntimeException("hello exception");
    }
}