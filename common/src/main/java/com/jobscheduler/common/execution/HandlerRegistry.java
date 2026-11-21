package com.jobscheduler.common.execution;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/** All JobHandler beans, by type. Fails at startup if two handlers claim the same type. */
@Component
public class HandlerRegistry {

    private final Map<String, JobHandler> byType;

    public HandlerRegistry(List<JobHandler> handlers) {
        this.byType = handlers.stream().collect(Collectors.toUnmodifiableMap(JobHandler::type, Function.identity()));
    }

    public boolean supports(String type) {
        return byType.containsKey(type);
    }

    public Set<String> types() {
        return byType.keySet();
    }

    public JobHandler get(String type) {
        JobHandler handler = byType.get(type);
        if (handler == null) {
            throw new NonRetryableException("No handler for job type " + type);
        }
        return handler;
    }
}
