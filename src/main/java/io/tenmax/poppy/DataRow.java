package io.tenmax.poppy;

import java.util.Date;
import java.util.Iterator;

public interface DataRow extends Iterable {

    DataColumn[] getColumns();

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

    default
    Iterator iterator() {
        return new Iterator() {
            private int i = 0;
            private int n = getColumns().length;

            @Override
            public boolean hasNext() {
                return i < n;
            }

            @Override
            public Object next() {
                return get(i++);
            }
        };
    }
}
