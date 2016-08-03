# Poppy
*poppy* is dataframe library for java, which provides common SQL operations (e.g. select, from, where, group by, order by, distinct) to process data in java.

Unlike other dataframe libraries, which keep all the data in memory, *poppy* process data in streaming manager. That is, it is more similar as [Java8 Stream library](https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html), but relational version.

Here is a simple example. We have a `Student` class

```java
public class Student {
    private int studentId;
    private String name;
    private int grade;
    private int room;
    private int height;
    private int weight;
    ...
}
```

In SQL, we have a query like this

```sql
select 
    grade, 
    room, 
    avg(weight) as weight, 
    avg(height) as height
from Student
group by grade, room
order by grade, room
```

Here is the *Poppy*'s version 

```java
List<Student> students = ...;

DataFrame
.from(students, Student.class)
.groupby("grade", "room")
.aggregate(
    avgLong("weight").as("weight"),
    avgLong("height").as("height"))
.sort("grade", "room")
.print();
```



# Getting Started

## Requirement
Java 8 or higher

## Dependency

Poppy's package is managed by [JCenter](https://bintray.com/bintray/jcenter) repository.

Maven

```
<dependency>
  <groupId>io.tenmax</groupId>
  <artifactId>poppy</artifactId>
  <version>0.1.6</version>
  <type>pom</type>
</dependency>
```

Gradle

```
compile 'io.tenmax:poppy:0.1.6'
```
## Features

1. Support the most common operations in SQL. e.g. select, from, where, group by, order by, distinct
2. Support the most common aggregation functions in SQL. e.g. *avg()*, *sum()*, *count()*, *min()*, *max()*
3. **Custom aggregation functions.** by  [java.util.stream.Collector](https://docs.oracle.com/javase/8/docs/api/java/util/stream/Collector.html)
4. **Partition support.** Partition is the unit of parallelism. Multiple partitions allow you processing data concurrently.
5. **Multi-threaded support**. For CPU-bound jobs, it leverages all your CPU resources for better performance; for IO-bound jobs, it reduces the waiting time, and take adventages of better concurrency.
6. Suitable for both **batch** and **streaming** scenario.
7. **Lightweight**. Comparing to [Spark DataFrame API](https://spark.apache.org/docs/latest/sql-programming-guide.html), it is much more lightweight to embed in your application.
8. **Stream-based design**. Comparing to [joinery](https://github.com/cardillo/joinery), which keeps the whole data in memory. *Poppy*'s streaming behaviour allows limited memory to process huge volume of data.

## Documentation

- [JavaDoc](http://tenmax.github.io/poppy/docs/javadoc/index.html)
- [User Manual](http://tenmax.github.io/poppy/)

# Contribution

Please fork this project and pull request to me and any comment would be appreciated!





