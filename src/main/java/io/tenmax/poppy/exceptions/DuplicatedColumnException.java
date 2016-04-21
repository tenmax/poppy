package io.tenmax.poppy.exceptions;

/**
 * Created by popcorny on 4/20/16.
 */
public class DuplicatedColumnException extends RuntimeException{

    public DuplicatedColumnException(String column) {
        super("Column Duplicated : " + column);
    }
}
