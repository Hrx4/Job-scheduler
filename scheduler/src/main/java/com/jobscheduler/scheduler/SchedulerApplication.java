package com.jobscheduler.scheduler;

import com.jobscheduler.common.messaging.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SchedulerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SchedulerApplication.class, args);
    }

    /** Created on startup if missing. Partition count caps worker parallelism. */
    @Bean
    NewTopic jobsTopic() {
        return TopicBuilder.name(Topics.JOBS)
                .partitions(Topics.JOBS_PARTITIONS)
                .replicas(1)
                .build();
    }
}
