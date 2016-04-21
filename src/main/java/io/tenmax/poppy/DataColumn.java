package io.tenmax.poppy;

public class DataColumn {
    private final String name;
    private final Class type;

    public DataColumn(String name, Class type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Class getType() {
        return type;
    }
}
