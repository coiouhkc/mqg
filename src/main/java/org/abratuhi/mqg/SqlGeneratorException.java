package org.abratuhi.mqg;

/**
 * Created by abratuhi on 13.06.14.
 */
public class SqlGeneratorException extends Exception {

    public SqlGeneratorException() {}

    public SqlGeneratorException(String message) {super(message);}

    public SqlGeneratorException(Throwable throwable) {super(throwable);}

    SqlGeneratorException(String message, Throwable throwable) {super(message, throwable);}
}
