package biocode.fims.fuseki.query;

import java.net.URI;

/**
 * A filter element defines an individual filter unit
 */
public class FimsFilterCondition {
    public static Integer AND = 1;
    public static Integer OR = 2;   // not supported yet
    public static Integer NOT = 3;  // not supported yet

    public URI uriProperty;
    public String value;
    public Integer operation;

    public FimsFilterCondition(URI uriProperty, String value, Integer operation) {
        this.uriProperty = uriProperty;
        this.value = value;
        this.operation = operation;
    }
}

