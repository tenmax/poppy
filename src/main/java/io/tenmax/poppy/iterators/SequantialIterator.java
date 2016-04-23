package io.tenmax.poppy.iterators;

import io.tenmax.poppy.DataRow;
import io.tenmax.poppy.dataframes.ExecutionContext;

import java.util.Iterator;

public class SequantialIterator implements Iterator<DataRow> {
    private final ExecutionContext context;
    private final Iterator<DataRow>[] iterators;
    private int top;

    public SequantialIterator(ExecutionContext context, Iterator<DataRow>... iterators) {
        this.context = context;
        this.iterators = iterators;
    }

    @Override
    public boolean hasNext() {
        while (true) {
            if(context.isClosed()) {
                return false;
            }

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
