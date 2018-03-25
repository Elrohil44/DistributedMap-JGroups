package map;

import org.jgroups.util.Util;

import java.io.*;

public class MapMessage implements Serializable {
    private Operation operation;
    private String key;
    private String value;

    public MapMessage() {}

    public MapMessage setOperation(Operation operation) {
        this.operation = operation;
        return this;
    }

    public MapMessage setKey(String key) {
        this.key = key;
        return this;
    }

    public MapMessage setValue(String value) {
        this.value = value;
        return this;
    }

    public Operation getOperation() {
        return operation;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
