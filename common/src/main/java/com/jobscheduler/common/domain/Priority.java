package com.jobscheduler.common.domain;

/** Job priority. Stored as SMALLINT; lower value is picked first. */
public enum Priority {
    HIGH(0),
    NORMAL(1),
    LOW(2);

    private final short code;

    Priority(int code) {
        this.code = (short) code;
    }

    public short code() {
        return code;
    }

    public static Priority fromCode(int code) {
        for (Priority p : values()) {
            if (p.code == code) {
                return p;
            }
        }
        throw new IllegalArgumentException("Unknown priority code: " + code);
    }
}
