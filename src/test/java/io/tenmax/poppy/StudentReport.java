package io.tenmax.poppy;

/**
 * Created by popcorny on 4/24/16.
 */
public class StudentReport {
    int grade;
    int room;
    double weight;
    double height;

    public int getGrade() {
        return grade;
    }

    public void setGrade(int grade) {
        this.grade = grade;
    }

    public int getRoom() {
        return room;
    }

    public void setRoom(int room) {
        this.room = room;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public double getHeight() {
        return height;
    }

    public void setHeight(double height) {
        this.height = height;
    }

    @Override
    public String toString() {
        return "StudentReport{" +
                "grade=" + grade +
                ", room=" + room +
                ", weight=" + weight +
                ", height=" + height +
                '}';
    }
}
