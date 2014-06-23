package org.abratuhi.mqg;

/**
 * Created by abratuhi on 23.06.14.
 */
public enum Operator {
    EQUALS, NOT_EQUALS, LIKE, CONTAINS, BETWEEN, IN, LESS, LESS_OR_EQUAL, GREATER, GREATER_OR_EQUAL;

    @Override
    public String toString() {
        switch (this) {
            case EQUALS: return "=";
            case NOT_EQUALS: return "<>";
            case LESS: return "<";
            case LESS_OR_EQUAL: return "<=";
            case GREATER: return ">";
            case GREATER_OR_EQUAL: return ">=";
            default: return super.toString();
        }
    }
}
