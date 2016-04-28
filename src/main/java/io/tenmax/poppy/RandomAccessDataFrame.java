package io.tenmax.poppy;

public interface RandomAccessDataFrame extends DataFrame {
    int size();

    DataRow getRow(int row);
}
