package com.jobscheduler.worker;

import java.net.InetAddress;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * This instance's lease_owner. Must be unique per running process, even for two workers on one host,
 * so the id gets a random suffix. Stored in VARCHAR(64).
 */
@Component
public class WorkerIdentity {

    private final String id;

    public WorkerIdentity(@Value("${worker.id:}") String configured) {
        String base = configured.isBlank() ? hostname() : configured;
        String full = base + "-" + UUID.randomUUID().toString().substring(0, 8);
        this.id = full.length() <= 64 ? full : full.substring(full.length() - 64);
    }

    public String id() {
        return id;
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "worker";
        }
    }
}
