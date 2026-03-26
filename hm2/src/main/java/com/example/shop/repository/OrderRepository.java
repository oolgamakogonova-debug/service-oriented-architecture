package com.example.shop.repository;

import com.example.shop.entity.Order;
import com.example.shop.entity.enums.OrderStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface OrderRepository extends JpaRepository<Order, UUID> {

    Page<Order> findByUserId(UUID userId, Pageable pageable);

    @Query("SELECT COUNT(o) FROM Order o WHERE o.userId = :userId AND o.status IN :statuses")
    long countByUserIdAndStatusIn(@Param("userId") UUID userId,
                                  @Param("statuses") List<OrderStatus> statuses);
}