package io.tenmax.poppy.dataframes;

import io.tenmax.poppy.DataRow;

import java.util.Iterator;

public class SequantialIterator implements Iterator<DataRow> {
    private final Iterator<DataRow>[] iterators;
    private int top;

    public SequantialIterator(Iterator<DataRow>... iterators) {
        this.iterators = iterators;
    }

    @Override
    public boolean hasNext() {
        while (true) {
            if (top >= iterators.length) {
                return false;
            }

            if (iterators[top].hasNext()) {
                return true;
            } else {
                top++;
            }
        }
    }

    @Override
    public DataRow next() {
        return iterators[top].next();
    }
}
