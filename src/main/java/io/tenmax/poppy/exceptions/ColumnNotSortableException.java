package io.tenmax.poppy.exceptions;

/**
 * Created by popcorny on 4/20/16.
 */
public class ColumnNotSortableException extends RuntimeException{
    public ColumnNotSortableException(String column) {
        super("Column not sortable: " + column);
    }
}
