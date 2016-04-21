package io.tenmax.poppy.exceptions;

public class ColumnNotFoundException extends RuntimeException{

    public ColumnNotFoundException(String column) {
        super("Column not found: " + column);
    }


}
