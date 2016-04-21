package io.tenmax.poppy;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.stream.Collectors;

import static io.tenmax.poppy.SpecUtils.*;

public class BaseDataFrameTest extends TestCase {

    private ArrayList<Student> list;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        list = new ArrayList<>();
        list.add(new Student(1, "pop", 5,2,170,60));
        list.add(new Student(2, "foo", 5,3,175,70));
        list.add(new Student(3, "bar", 5,4,168,80));
        list.add(new Student(4, "john", 5,4,160,60));
    }

    public void testBasic() throws Exception {
        DataFrame
        .from(list, Student.class)
        .print();
    }

    public void testProject() throws Exception {
        DataFrame
        .from(list, Student.class)
        .project("name", "weight", "height")
        .print();
    }

    public void testProject2() throws Exception {
        DataFrame
        .from(list, Student.class)
        .project(
            col("name"),
            colMap("weight").as("w"),
            colMap("height", Float.class, (Integer height) -> (height / 3f)).as("h"))
        .print();
    }

    public void testFilter() throws Exception {
        DataFrame
        .from(list, Student.class)
        .filter(row -> row.getInteger("height") >= 170)
        .project("name", "weight", "height")
        .print();
    }

    public void testAggre() throws Exception {
        DataFrame
        .from(list, Student.class)
        .aggregate(
                avgLong("weight").as("weight"),
                avgLong("height").as("height"),
                count().as("count"),
                aggreMap("weight", Integer.class, Collectors.summingInt((Integer i) -> i)).as("wi"))
                .print();
    }

    public void testGroupBy() throws Exception {
        DataFrame
        .from(list, Student.class)
        .groupby("grade", "room")
        .aggregate(
            avgLong("weight").as("weight"),
            avgLong("height").as("height"))
        .print();
    }

    public void testSort() throws Exception {
        DataFrame
        .from(list, Student.class)
        .sort(asc("weight"), desc("height"))
        .print();
    }

    public void testSort2() throws Exception {
        DataFrame
                .from(list, Student.class)
                .groupby("grade", "room")
                .aggregate(
                        avgLong("weight").as("weight"),
                        avgLong("height").as("height"))
                .sort(desc("weight"), desc("height"))
                .print();
    }

    public void testDistinct() throws Exception {
        DataFrame
        .from(list, Student.class)
        .distinct("grade", "room")
        .print();
    }


}
