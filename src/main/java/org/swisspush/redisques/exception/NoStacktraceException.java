package org.swisspush.redisques.exception;

/**
 * Basically same as in vertx, But adding forgotten constructors.
 */
public class NoStacktraceException extends RuntimeException {

    public NoStacktraceException() {
        this(null, null, false);
    }

    public NoStacktraceException(String message) {
        this(message, null, false);
    }

    public NoStacktraceException(String message, Throwable cause) {
        this(message, cause, false);
    }

    public NoStacktraceException(Throwable cause) {
        this(null, cause, false);
    }

    public NoStacktraceException(String message, Throwable cause, boolean withStacktrace) {
        super(message, cause, true, withStacktrace);
    }

}
