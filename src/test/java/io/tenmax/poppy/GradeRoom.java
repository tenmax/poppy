package io.tenmax.poppy;

/**
 * Created by popcorny on 4/28/16.
 */
public class GradeRoom {
    int grade;
    int room;

    public GradeRoom() {
    }

    public GradeRoom(int grade, int room) {
        this.grade = grade;
        this.room = room;
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GradeRoom gradeRoom = (GradeRoom) o;

        if (grade != gradeRoom.grade) return false;
        if (room != gradeRoom.room) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = grade;
        result = 31 * result + room;
        return result;
    }

    @Override
    public String toString() {
        return "GradeRoom{" +
                "grade=" + grade +
                ", room=" + room +
                '}';
    }
}
