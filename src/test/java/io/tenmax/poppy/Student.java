package io.tenmax.poppy;

/**
 * Created by popcorny on 4/17/16.
 */
public class Student {
    private int studentId;
    private String name;
    private int grade;
    private int room;
    private int height;
    private int weight;

    public Student() {
    }



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

    public void setGrade(int grade) {
        this.grade = grade;
    }

    public int getGrade() {
        return grade;
    }

    public void setRoom(int room) {
        this.room = room;
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

    @Override
    public String toString() {
        return "Student{" +
                "studentId=" + studentId +
                ", name='" + name + '\'' +
                ", grade=" + grade +
                ", room=" + room +
                ", height=" + height +
                ", weight=" + weight +
                '}';
    }
}
