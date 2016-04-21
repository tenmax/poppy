package io.tenmax.poppy;

import java.util.Date;

/**
 * Created by popcorny on 4/10/16.
 */
public interface DataRow {
    Object get(int index);

    Object get(String name);

    default boolean getBoolean(int index) {
        return ((Boolean) get(index)).booleanValue();
    }

    default boolean getBoolean(String name) {
        return ((Boolean) get(name)).booleanValue();
    }

    default int getInteger(int index) {
        return ((Number) get(index)).intValue();
    }

    default int getInteger(String name) {
        return ((Number) get(name)).intValue();
    }

    default long getLong(int index) {
        return ((Number) get(index)).longValue();
    }

    default long getLong(String name) {
        return ((Number) get(name)).longValue();
    }

    default float getFloat(int index) {
        return ((Number) get(index)).floatValue();
    }

    default float getFloat(String name) {
        return ((Number) get(name)).floatValue();
    }

    default double getDouble(int index) {
        return ((Number) get(index)).doubleValue();
    }

    default double getDouble(String name) {
        return ((Number) get(name)).doubleValue();
    }

    default String getString(String name) {
        return ((String) get(name));
    }

    default String getString(int index) {
        return ((String) get(index));
    }

    default Date getDate(int index) {
        return ((Date) get(index));
    }

    default Date getDate(String name) {
        return ((Date) get(name));
    }
}
