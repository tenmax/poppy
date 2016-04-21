package io.tenmax.poppy;

public class SortSpec {
    public enum Order {
        ASC, DESC
    };

    private final String column;
    private final Order order;

    public SortSpec(String column, Order order) {
        this.column = column;
        this.order = order;
    }

    public String getColumn() {
        return column;
    }

    public Order getOrder() {
        return order;
    }
}
