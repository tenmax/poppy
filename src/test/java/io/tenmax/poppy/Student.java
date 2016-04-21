package io.tenmax.poppy;

/**
 * Created by popcorny on 4/17/16.
 */
public class Student {
    private final int studentId;
    private final String name;
    private final int grade;
    private final int room;
    private final int height;
    private final int weight;

    public Student(int studentId, String name, int grade, int room,  int height, int weight) {
        this.studentId = studentId;
        this.grade = grade;
        this.room = room;
        this.name = name;
        this.height = height;
        this.weight = weight;
    }

    public int getStudentId() {
        return studentId;
    }

    public String getName() {
        return name;
    }

    public int getGrade() {
        return grade;
    }

    public int getRoom() {
        return room;
    }

    public int getHeight() {
        return height;
    }

    public int getWeight() {
        return weight;
    }
}
