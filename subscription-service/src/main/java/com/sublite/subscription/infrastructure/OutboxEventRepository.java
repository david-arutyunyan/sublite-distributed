package com.sublite.subscription.infrastructure;

import com.sublite.subscription.domain.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, java.util.UUID> {

    /**
     * FOR UPDATE SKIP LOCKED, not a plain SELECT: this repository will
     * eventually be polled by more than one pod (Kubernetes, step 11) -
     * without SKIP LOCKED, two pollers running at once would both select
     * the same unpublished rows and race to publish them twice
     * (duplicates are already fine, consumers are idempotent - but two
     * pollers BLOCKING each other on the same locked rows, one waiting
     * for the other's transaction to finish, is wasted work with no
     * upside). SKIP LOCKED makes each poller instance just take whatever
     * rows aren't already claimed by another instance's in-flight
     * transaction, instead of queueing behind it.
     */
    @Query(value = """
            SELECT * FROM outbox_events
            WHERE published_at IS NULL
            ORDER BY created_at ASC
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEvent> findBatchToPublish(@Param("batchSize") int batchSize);
}
