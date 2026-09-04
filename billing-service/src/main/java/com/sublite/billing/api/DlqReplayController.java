package com.sublite.billing.api;

import com.sublite.billing.application.DlqReplayService;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * An ops tool, not a customer-facing API - no auth on it yet because this
 * project doesn't have any auth layer at all (see subscription-service's
 * own scope-cut notes); a real deployment would put this behind whatever
 * restricts access to internal tooling, not open it publicly.
 */
@RestController
@RequestMapping("/admin/dlq")
public class DlqReplayController {

    private final DlqReplayService replayService;

    public DlqReplayController(DlqReplayService replayService) {
        this.replayService = replayService;
    }

    @PostMapping("/{topic}/replay")
    public Map<String, Object> replay(@PathVariable String topic) {
        int replayedCount = replayService.replay(topic);
        return Map.of("topic", topic, "replayedCount", replayedCount);
    }
}
