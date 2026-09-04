package com.sublite.notification.api;

import com.sublite.notification.application.DlqReplayService;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * An ops tool, not a customer-facing API - see billing-service's own
 * DlqReplayController for why this is unauthenticated for now.
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
