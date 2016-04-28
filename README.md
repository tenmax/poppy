# Poppy
*poppy* is dataframe library for java. It provides common SQL operations (e.g. select, from, where, group by, order by, distinct) to process data in java.

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
grade, room, avg(weight) as weight, avg(height) as height
from Student
group by grade, room
order by grade, room
```

Here is the corresponding code in *poppy*

```java
List<Student> students = ...;

DataFrame
.from(Student.class, students)
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

Maven

```
<dependency>
  <groupId>io.tenmax</groupId>
  <artifactId>poppy</artifactId>
  <version>0.1.0</version>
  <type>pom</type>
</dependency>
```

Gradle

```
compile 'io.tenmax:poppy:0.1.0'
```
## Features

1. Support the most common operations in SQL. e.g. select, from, where, group by, order by, distinct
2. Support the most common aggregation functions in SQL. e.g. *avg()*, *sum()*
3. **Custom aggregation functions.** by  [java.util.stream.Collector](https://docs.oracle.com/javase/8/docs/api/java/util/stream/Collector.html)
4. **Partition support.** Partition is the unit of parallelism. Multiple partitions allow you processing data concurrently.
5. **Multi-threaded support.** No matter for IO-bound or CPU-bound tasks,  multi-threaded processing leverage all your CPU resources.
6. Suitable for both **batch** and **streaming** scenario.


## Documentation

JavaDoc


# Input

There are two kinds of input.

1. DataFrame.from(Class\<T> clazz, java.util.Iterable... iterables)
2. DataFrame.from(io.tenmax.DataSource dataSource)

The first one uses JavaBean convention to define the table schema. This is the simplest way to create a dataframe

```
List<Student> students = ...;

DataFrame df = DataFrame.from(Student.class, students);
```

The second one allow you most flexible way to define the data source. In data source, it should define 

1. The schema (by a list of column)
2. Partition count
3. The iterators for specified partition
4. The mapping from one data to data in all columns.

```
DataSource dataSource = ...

DataFrame df = DataFrame.from(dataSource);
```

# Output

There are several ways to output the data

1. Iterator
2. forEach
3. toList
4. toMap
5. DataFrame.to(DataSink dataSink)

The first two methods overriding the *java.util.Iterable* iterface. So you can use `for(T t: dataFrame)` to iterate through the dataframe.

`toList` and `toMap` provide a quick methods to output dataframe to a collection. Both provide the reflection version, which export the data by the JavaBean conversion. `toMap` ues the `groupby` method to define the key of map.

The latest version provide the most flexible version of output. It also allow output the result directly in the multi-threaded context. It offers the best level of parallel to output the data to destination, just like Hadoop does.

# Operations

## Project
Project is the same as `select` in SQL. Project map one column to another name, or merge multiple columns to one column. We use SQL statement as analogy example.

*SQL*

```sql
select name, weight, height from student;
```

*Poppy*

```java
df.project("name", "weight", "height");
```

Another example is to aliasing a name to a column.

*SQL*

```sql
select 
    name, 
    weight as w, 
    height / 10 as h
from student;
```

*Poppy*

```java
import static io.tenmax.poppy.SpecUtils.*;

df.project(
   col("name"),
   colMap("weight").as("w"),
   colMap("height", Float.class, (Integer height) -> (height / 10f)).as("h"))
```

## Filter
Filter is the same as `where` or `having` in SQL. Filter is used to keep the row which passes the rule.

*SQL*

```sql
select * from Student where height > 170;
```

*Poppy*

```java
df.filter(row -> row.getInteger("height") >= 170);
```


## Aggregation

*SQL*

```sql
select 
    count(*) as c,
    avg(weight) as weight,
    avg(height) as height
from Student;
```
*Poppy*

```java
df.aggregate(
	count().as("c")
    avgLong("weight").as("weight"),
    avgLong("height").as("height")
);
```

You can have your custom aggregation by Java8 [Collector](https://docs.oracle.com/javase/8/docs/api/java/util/stream/Collector.html) interface.

```java
df.aggregate(
    aggreMap("weight", Integer.class, Collectors.summingInt((Integer i) -> i)).as("wi"))
)
```

Of course, poppy support aggregate with grouping.


```sql
select 
    grade, 
    room, 
    avg(weight) as weight, 
    avg(height) as height
from Student
group by grade, room
```

Here is the corresponding code in *poppy*

```java
df
.groupby("grade","room")
.aggregate(
    avgLong("weight").as("weight"),
    avgLong("height").as("height"));
```

## Sort

Sort the dataframe by columns.

*SQL*

```sql
select *
from Student
order by weight, height;
```
*Poppy*

```java
df.sort("weight", "height");
```

Specify the sorting orders.

*SQL*

```sql
select *
from Student
order by weight asc, height desc;
```
*Poppy*

```java
import static io.tenmax.poppy.SpecUtils.*;

df.sort(asc("weight"), desc("height"));
```


## Distinct

Select the unique records by columns.

*SQL*

```sql
select distinct grade, room from Student;
```
*Poppy*

```java
df.distinct("grade", "room");
```

# Partition and Parallelism

Partition is the unit of parallelism. To leverage the multicore computing power, we can provide more than one partitions in the data sources. And in dataframe, we use the `parallel(n)` to define number of threads running in the thread pool

```java
DataFrame
.from(myDataSource)
.parallel(4)
.aggregate(...)
.forEach(row -> {/*...*/})
```

## Execution Context

*Poppy* introduces the concept of execution context. One execution context contains a thread pool with *n* threads and *m* partitions. It treat one partition as a task, and one thread only proceeds task at the same time. Internally, it make the projection, filtering, accumulation of aggregation as a pipeline. Once all tasks complete, the thread pool shutdown and release all the resources. 

The following diagram is an example of multiple partition with 3 threads in pool. And the final results would pipe the result to the queue and be pulled by the caller thread. 

![](assets/executionContext.png)

Another case is the DataFrame output the result to a DataSink directly. Here the result is invoked in the threads of execution context.

![](assets/executionContext2.png)

If there is no parallel threads defined in the execution context, the default behaviour is to use the caller thread to iterate through all the partition sequentially. So there is no thread spawned in this case.

![](assets/executionContext3.png)

For some operations, they would create a new execution context. Such as, aggregation, sort, distinct. Currently, they would create a new context with only one partition. The following example is a aggregation example, the partition data would be accumulated as a accumulated value and be combined all the results to the final result.

![](assets/executionContext4.png)






