package com.jobscheduler.api.web;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.UUID;

/** Opaque keyset cursor: the (timestamp, id) of the last row on the previous page. */
public record Cursor(OffsetDateTime at, UUID id) {

    public String encode() {
        String raw = at + "|" + id;
        return Base64.getUrlEncoder().withoutPadding().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    /** Returns null for a null or empty cursor. */
    public static Cursor decode(String cursor) {
        if (cursor == null || cursor.isEmpty()) {
            return null;
        }
        try {
            String raw = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8);
            String[] parts = raw.split("\\|", 2);
            return new Cursor(OffsetDateTime.parse(parts[0]), UUID.fromString(parts[1]));
        } catch (RuntimeException e) {
            throw ApiException.badRequest("INVALID_CURSOR", "cursor is not valid");
        }
    }
}
