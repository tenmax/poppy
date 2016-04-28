package io.tenmax.poppy;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.tenmax.poppy.SpecUtils.*;

public class DataFrameTest extends TestCase {

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
        Iterator<DataRow> it = DataFrame
        .from(list, Student.class)
        .iterator();

        assertEquals("pop", it.next().getString("name"));
        assertEquals(5, it.next().getInteger("grade"));
        assertEquals(168, it.next().getInteger("height"));
        assertEquals(60, it.next().getInteger("weight"));
    }

    public void testProject() throws Exception {
        Iterator<DataRow> it = DataFrame
        .from(list, Student.class)
        .project("name", "weight", "height")
        .iterator();

        assertEquals("pop", it.next().getString(0));
        assertEquals(70, it.next().getInteger(1));
        assertEquals(168, it.next().getInteger(2));
        assertEquals(160, it.next().getInteger("height"));
    }

    public void testProject2() throws Exception {
        Iterator<DataRow> it = DataFrame
        .from(list, Student.class)
        .project(
            col("name"),
            colMap("weight").as("w"),
            colMap("height", Float.class, (Integer height) -> (height / 10f)).as("h"))
        .iterator();

        assertEquals("pop", it.next().getString(0));
        assertEquals(70, it.next().getInteger("w"));
        assertEquals(16.8f, it.next().getFloat("h"), 0.1);
        assertEquals(16.0f, it.next().getFloat(2), 0.1);
    }

    public void testFilter() throws Exception {
        Iterator<DataRow> it = DataFrame
        .from(list, Student.class)
        .filter(row -> row.getInteger("height") >= 170)
        .project("name", "weight", "height")
        .iterator();

        assertEquals("pop", it.next().getString(0));
        assertEquals(175, it.next().getInteger(2));
        assertEquals(false, it.hasNext());
    }

    public void testAggre() throws Exception {
        Iterator<DataRow> it = DataFrame
        .from(list, Student.class)
        .aggregate(
                avgLong("weight").as("weight"),
                avgLong("height").as("height"),
                count().as("count"),
                aggreMap("weight", Integer.class, Collectors.summingInt((Integer i) -> i)).as("wi"))
                .iterator();

        DataRow row = it.next();
        assertEquals(row.getDouble("weight"), 67.5, 0.1);
        assertEquals(row.getDouble("height"), 168.25, 0.1);
        assertEquals(row.getLong("count"), 4);
        assertEquals(row.getInteger("wi"), 270);
    }

    public void testGroupBy() throws Exception {
        Iterator<DataRow> it = DataFrame
        .from(list, Student.class)
        .groupby("grade", "room")
        .aggregate(
            avgLong("weight").as("weight"),
            avgLong("height").as("height"))
        .sort("grade", "room")
        .iterator();

        assertEquals(2, it.next().getInteger("room"));
        assertEquals(70.0, it.next().getDouble("weight"), 0.1);
        assertEquals(164.0, it.next().getDouble("height"), 0.1);
    }

    public void testSort() throws Exception {
        Iterator<DataRow> it = DataFrame
                .from(list, Student.class)
                .sort("weight", "height")
                .iterator();

        assertEquals(4, it.next().getInteger("studentId"));
        assertEquals(1, it.next().getInteger("studentId"));
        assertEquals(2, it.next().getInteger("studentId"));
        assertEquals(3, it.next().getInteger("studentId"));
    }

    public void testSort2() throws Exception {
        Iterator<DataRow> it = DataFrame
        .from(list, Student.class)
        .sort(asc("weight"), desc("height"))
        .iterator();

        assertEquals(1, it.next().getInteger("studentId"));
        assertEquals(4, it.next().getInteger("studentId"));
        assertEquals(2, it.next().getInteger("studentId"));
        assertEquals(3, it.next().getInteger("studentId"));
    }

    public void testDistinct() throws Exception {
        Iterator<DataRow> it = DataFrame
        .from(list, Student.class)
        .distinct("grade", "room")
        .iterator();

        assertEquals(2, it.next().getInteger("room"));
        assertEquals(3, it.next().getInteger("room"));
        assertEquals(4, it.next().getInteger("room"));
        assertEquals(false, it.hasNext());
    }

    public void testCache() throws Exception {
        RandomAccessDataFrame cache = DataFrame.from(list, Student.class)
                .cache();

        assertEquals(4, cache.size());
        assertEquals(2, cache.getRow(1).getInteger("studentId"));
        assertEquals(80, cache.getRow(2).getInteger("weight"));
        assertEquals("john", cache.getRow(3).getString("name"));
    }



    public void testToList() throws Exception {
        List<StudentReport> studentReports =
        DataFrame
        .from(list, Student.class)
        .groupby("grade", "room")
        .aggregate(
                avgLong("weight").as("weight"),
                avgLong("height").as("height"))
        .sort("grade", "room")
        .toList(StudentReport.class);

        StudentReport report = studentReports.get(0);
        assertEquals(5, report.getGrade());
        assertEquals(2, report.getRoom());
        assertEquals(60.0, report.getWeight(),0.1);
        assertEquals(170.0, report.getHeight(),0.1);

    }

    public void testToMap() throws Exception {

        Map<GradeRoom, StudentReport> reportMap =
        DataFrame
        .from(list, Student.class)
        .groupby("grade", "room")
        .aggregate(
                avgLong("weight").as("weight"),
                sumLong("weight").as("weightTotal"),
                avgLong("height").as("height"),
                sumLong("height").as("heightTotal"))
        .sort("grade", "room")
        .toMap(GradeRoom.class, StudentReport.class);

        reportMap.forEach((key, value) -> {
            System.out.println(key);
            System.out.println(value);
        });

        assertEquals(reportMap.get(new GradeRoom(5,2)).getWeight(), 60.0);
        assertEquals(reportMap.get(new GradeRoom(5,3)).getHeight(), 175.0);
        assertEquals(reportMap.get(new GradeRoom(5,4)).getHeight(), 164.0);
    }


}
