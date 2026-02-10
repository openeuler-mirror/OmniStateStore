package com.huawei.falcon.state.cache;

import org.rocksdb.Status;

/**
 * A FalconException encapsulates the error of an operation. This exception type is used to describe an internal error
 * from the c++ falcon library.
 */
public class FalconException extends Exception {
    /* @Nullable */ private final Status status;

    /**
     * The private construct used by a set of public static factory method.
     *
     * @param msg the specified error message.
     */
    public FalconException(final String msg) {
        this(msg, null);
    }

    public FalconException(final String msg, final Status status) {
        super(msg);
        this.status = status;
    }

    public FalconException(final Status status) {
        super(status.getState() != null ? status.getState() : status.getCodeString());
        this.status = status;
    }

    /**
     * Get the status returned from FalconValueState
     *
     * @return The status reported by Falcon, or null if no status is available
     */
    public Status getStatus() {
        return status;
    }
}