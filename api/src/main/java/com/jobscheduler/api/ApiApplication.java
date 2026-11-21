package com.jobscheduler.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Scans com.jobscheduler so the shared repositories, executor and handlers in common are picked up. */
@SpringBootApplication(scanBasePackages = "com.jobscheduler")
@EnableScheduling
public class ApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(ApiApplication.class, args);
    }
}
