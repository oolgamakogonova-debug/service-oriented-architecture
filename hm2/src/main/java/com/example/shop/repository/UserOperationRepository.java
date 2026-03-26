package com.example.shop.repository;

import com.example.shop.entity.UserOperation;
import com.example.shop.entity.enums.OperationType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface UserOperationRepository extends JpaRepository<UserOperation, UUID> {

    @Query("SELECT MAX(uo.createdAt) FROM UserOperation uo " +
           "WHERE uo.userId = :userId AND uo.operationType IN :types")
    Instant findLastOperationTime(@Param("userId") UUID userId,
                                  @Param("types") List<OperationType> types);
}