package com.kafka.order.dlt.repository;

import com.kafka.order.dlt.model.FailedOrderLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FailedOrderLogRepository extends JpaRepository<FailedOrderLog, Long> {
}