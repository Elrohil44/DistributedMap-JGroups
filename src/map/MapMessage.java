package map;

import org.jgroups.Address;

import java.io.*;

public class MapMessage implements Serializable {
    private Operation operation;
    private String key;
    private String value;
    private Address stateAddress;

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

    public MapMessage setStateAddress(Address stateAddress) {
        this.stateAddress = stateAddress;
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

    public Address getStateAddress() {
        return stateAddress;
    }
}
