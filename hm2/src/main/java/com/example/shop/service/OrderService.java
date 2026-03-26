package com.example.shop.service;

import com.example.shop.dto.PageResponse;
import com.example.shop.dto.order.*;
import com.example.shop.entity.*;
import com.example.shop.entity.enums.*;
import com.example.shop.exception.*;
import com.example.shop.repository.*;
import com.example.shop.security.UserPrincipal;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final ProductRepository productRepository;
    private final PromoCodeRepository promoCodeRepository;
    private final UserOperationRepository userOperationRepository;

    @Value("${app.order.rate-limit-seconds}")
    private long rateLimitSeconds;

    // ============ CREATE ORDER ============
    @Transactional
    public OrderResponse createOrder(OrderCreateRequest request, UserPrincipal principal) {
        UUID userId = principal.getId();

        // 1. Check role
        if (principal.getRole() == UserRole.SELLER) {
            throw new AccessDeniedException("SELLER cannot create orders");
        }

        // 2. Rate limit
        checkRateLimit(userId, List.of(OperationType.CREATE_ORDER));

        // 3. Check active orders
        long activeCount = orderRepository.countByUserIdAndStatusIn(userId,
                List.of(OrderStatus.CREATED, OrderStatus.PAYMENT_PENDING));
        if (activeCount > 0) {
            throw new TooManyActiveOrdersException();
        }

        // 4. Validate products & stock
        List<Product> products = new ArrayList<>();
        List<Map<String, Object>> insufficientItems = new ArrayList<>();

        for (OrderItemRequest item : request.getItems()) {
            Product product = productRepository.findById(item.getProductId())
                    .orElseThrow(() -> new ProductNotFoundException(item.getProductId()));

            if (product.getStatus() != ProductStatus.ACTIVE) {
                throw new ProductNotActiveException(item.getProductId());
            }

            if (product.getStock() < item.getQuantity()) {
                insufficientItems.add(Map.of(
                        "productId", product.getId().toString(),
                        "requested", item.getQuantity(),
                        "available", product.getStock()
                ));
            }
            products.add(product);
        }

        if (!insufficientItems.isEmpty()) {
            throw new InsufficientStockException(insufficientItems);
        }

        // 5. Reserve stock & create order items
        Order order = Order.builder()
                .userId(userId)
                .status(OrderStatus.CREATED)
                .totalPrice(BigDecimal.ZERO)
                .build();
        order = orderRepository.save(order);

        BigDecimal totalAmount = BigDecimal.ZERO;
        List<OrderItem> orderItems = new ArrayList<>();

        for (int i = 0; i < request.getItems().size(); i++) {
            OrderItemRequest itemReq = request.getItems().get(i);
            Product product = products.get(i);

            // Reserve stock
            product.setStock(product.getStock() - itemReq.getQuantity());
            productRepository.save(product);

            // Create order item with price snapshot
            OrderItem orderItem = OrderItem.builder()
                    .orderId(order.getId())
                    .productId(product.getId())
                    .productName(product.getName())
                    .quantity(itemReq.getQuantity())
                    .priceAtOrder(product.getPrice())
                    .build();

            orderItems.add(orderItem);
            totalAmount = totalAmount.add(
                    product.getPrice().multiply(BigDecimal.valueOf(itemReq.getQuantity())));
        }

        orderItemRepository.saveAll(orderItems);
        order.setItems(orderItems);

        // 6. Apply promo code
        order.setTotalPrice(totalAmount);
        if (request.getPromoCode() != null && !request.getPromoCode().isBlank()) {
            applyPromoCode(order, request.getPromoCode(), totalAmount);
        } else {
            order.setDiscountedPrice(totalAmount);
        }

        order = orderRepository.save(order);

        // 7. Record operation
        recordOperation(userId, OperationType.CREATE_ORDER, order.getId());

        return toResponse(order);
    }

    // ============ UPDATE ORDER ============
    @Transactional
    public OrderResponse updateOrder(UUID orderId, OrderUpdateRequest request, UserPrincipal principal) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        // 1. Ownership check
        checkOwnership(order, principal);

        // 2. State check
        if (order.getStatus() != OrderStatus.CREATED) {
            throw new InvalidStateTransitionException(order.getStatus().name(), "UPDATE");
        }

        // 3. Rate limit
        checkRateLimit(principal.getId(), List.of(OperationType.UPDATE_ORDER));

        // 4. Return old stock
        for (OrderItem item : order.getItems()) {
            Product product = productRepository.findById(item.getProductId())
                    .orElseThrow(() -> new ProductNotFoundException(item.getProductId()));
            product.setStock(product.getStock() + item.getQuantity());
            productRepository.save(product);
        }

        // 5. Validate and reserve new items
        List<Product> newProducts = new ArrayList<>();
        List<Map<String, Object>> insufficientItems = new ArrayList<>();

        for (OrderItemRequest item : request.getItems()) {
            Product product = productRepository.findById(item.getProductId())
                    .orElseThrow(() -> new ProductNotFoundException(item.getProductId()));

            if (product.getStatus() != ProductStatus.ACTIVE) {
                throw new ProductNotActiveException(item.getProductId());
            }

            if (product.getStock() < item.getQuantity()) {
                insufficientItems.add(Map.of(
                        "productId", product.getId().toString(),
                        "requested", item.getQuantity(),
                        "available", product.getStock()
                ));
            }
            newProducts.add(product);
        }

        if (!insufficientItems.isEmpty()) {
            throw new InsufficientStockException(insufficientItems);
        }

        // Remove old items
        order.getItems().clear();
        orderRepository.save(order); // flush orphan removal

        BigDecimal totalAmount = BigDecimal.ZERO;
        List<OrderItem> newOrderItems = new ArrayList<>();

        for (int i = 0; i < request.getItems().size(); i++) {
            OrderItemRequest itemReq = request.getItems().get(i);
            Product product = newProducts.get(i);

            product.setStock(product.getStock() - itemReq.getQuantity());
            productRepository.save(product);

            OrderItem orderItem = OrderItem.builder()
                    .orderId(order.getId())
                    .productId(product.getId())
                    .productName(product.getName())
                    .quantity(itemReq.getQuantity())
                    .priceAtOrder(product.getPrice())
                    .build();

            newOrderItems.add(orderItem);
            totalAmount = totalAmount.add(
                    product.getPrice().multiply(BigDecimal.valueOf(itemReq.getQuantity())));
        }

        orderItemRepository.saveAll(newOrderItems);
        order.setItems(newOrderItems);
        order.setTotalPrice(totalAmount);

        // 6. Recalculate promo
        if (order.getPromoCodeId() != null) {
            PromoCode promo = promoCodeRepository.findById(order.getPromoCodeId()).orElse(null);
            if (promo != null) {
                if (promo.getMinOrderAmount() != null && totalAmount.compareTo(promo.getMinOrderAmount()) < 0) {
                    // Promo no longer applicable — remove it
                    promo.setCurrentUses(promo.getCurrentUses() - 1);
                    promoCodeRepository.save(promo);
                    order.setPromoCodeId(null);
                    order.setDiscountedPrice(totalAmount);
                } else {
                    // Recalculate discount
                    BigDecimal discount = calculateDiscount(promo, totalAmount);
                    order.setDiscountedPrice(totalAmount.subtract(discount));
                }
            } else {
                order.setDiscountedPrice(totalAmount);
            }
        } else if (request.getPromoCode() != null && !request.getPromoCode().isBlank()) {
            applyPromoCode(order, request.getPromoCode(), totalAmount);
        } else {
            order.setDiscountedPrice(totalAmount);
        }

        order = orderRepository.save(order);

        // 7. Record operation
        recordOperation(principal.getId(), OperationType.UPDATE_ORDER, order.getId());

        return toResponse(order);
    }

    // ============ CANCEL ORDER ============
    @Transactional
    public OrderResponse cancelOrder(UUID orderId, UserPrincipal principal) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        // 1. Ownership
        checkOwnership(order, principal);

        // 2. State check
        if (order.getStatus() != OrderStatus.CREATED &&
            order.getStatus() != OrderStatus.PAYMENT_PENDING) {
            throw new InvalidStateTransitionException(order.getStatus().name(), "CANCEL");
        }

        // 3. Return stock
        for (OrderItem item : order.getItems()) {
            Product product = productRepository.findById(item.getProductId())
                    .orElseThrow(() -> new ProductNotFoundException(item.getProductId()));
            product.setStock(product.getStock() + item.getQuantity());
            productRepository.save(product);
        }

        // 4. Return promo usage
        if (order.getPromoCodeId() != null) {
            PromoCode promo = promoCodeRepository.findById(order.getPromoCodeId()).orElse(null);
            if (promo != null) {
                promo.setCurrentUses(promo.getCurrentUses() - 1);
                promoCodeRepository.save(promo);
            }
        }

        // 5. Set canceled
        order.setStatus(OrderStatus.CANCELED);
        order = orderRepository.save(order);

        recordOperation(principal.getId(), OperationType.CANCEL_ORDER, order.getId());

        return toResponse(order);
    }

    // ============ GET ORDER ============
    public OrderResponse getOrderById(UUID orderId, UserPrincipal principal) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        checkOwnership(order, principal);
        return toResponse(order);
    }

    // ============ GET MY ORDERS ============
    public PageResponse<OrderResponse> getMyOrders(UserPrincipal principal, int page, int size) {
        UUID userId = principal.getRole() == UserRole.ADMIN ? null : principal.getId();

        Page<Order> result;
        PageRequest pageable = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "createdAt"));

        if (userId != null) {
            result = orderRepository.findByUserId(userId, pageable);
        } else {
            result = orderRepository.findAll(pageable);
        }

        List<OrderResponse> content = result.getContent().stream()
                .map(this::toResponse)
                .toList();

        return PageResponse.<OrderResponse>builder()
                .content(content)
                .page(result.getNumber())
                .size(result.getSize())
                .totalElements(result.getTotalElements())
                .totalPages(result.getTotalPages())
                .build();
    }

    // ============ HELPERS ============

    private void checkRateLimit(UUID userId, List<OperationType> types) {
        Instant lastOp = userOperationRepository.findLastOperationTime(userId, types);
        if (lastOp != null) {
            long secondsSince = Duration.between(lastOp, Instant.now()).getSeconds();
            if (secondsSince < rateLimitSeconds) {
                throw new OrderRateLimitException();
            }
        }
    }

    private void checkOwnership(Order order, UserPrincipal principal) {
        if (principal.getRole() == UserRole.ADMIN) {
            return;
        }
        if (principal.getRole() == UserRole.SELLER) {
            throw new AccessDeniedException("SELLER cannot access orders");
        }
        if (!order.getUserId().equals(principal.getId())) {
            throw new OrderOwnershipViolationException();
        }
    }

    private void applyPromoCode(Order order, String code, BigDecimal totalAmount) {
        PromoCode promo = promoCodeRepository.findByCode(code)
                .orElseThrow(() -> new InvalidPromoCodeException("PROMO_CODE_INVALID", "Promo code not found: " + code));

        if (!promo.getActive()) {
            throw new InvalidPromoCodeException("PROMO_CODE_INVALID", "Promo code is not active");
        }
        if (promo.getMaxUses() != null && promo.getCurrentUses() >= promo.getMaxUses()) {
            throw new InvalidPromoCodeException("PROMO_CODE_INVALID", "Promo code has been fully used");
        }
        if (promo.getExpiresAt() != null && Instant.now().isAfter(promo.getExpiresAt())) {
            throw new InvalidPromoCodeException("PROMO_CODE_INVALID", "Promo code has expired");
        }
        if (promo.getMinOrderAmount() != null && totalAmount.compareTo(promo.getMinOrderAmount()) < 0) {
            throw new InvalidPromoCodeException("PROMO_CODE_MIN_AMOUNT",
                    "Order total " + totalAmount + " is below minimum " + promo.getMinOrderAmount());
        }

        BigDecimal discount = calculateDiscount(promo, totalAmount);
        BigDecimal discountedPrice = totalAmount.subtract(discount);

        order.setPromoCodeId(promo.getId());
        order.setDiscountedPrice(discountedPrice);

        promo.setCurrentUses(promo.getCurrentUses() + 1);
        promoCodeRepository.save(promo);
    }

    private BigDecimal calculateDiscount(PromoCode promo, BigDecimal totalAmount) {
        // Using discountPercent field for percentage discounts
        BigDecimal discount = totalAmount.multiply(promo.getDiscountPercent())
                .divide(BigDecimal.valueOf(100), 2, RoundingMode.HALF_UP);

        // Cap at 70% of total
        BigDecimal maxDiscount = totalAmount.multiply(BigDecimal.valueOf(0.70))
                .setScale(2, RoundingMode.HALF_UP);

        if (discount.compareTo(maxDiscount) > 0) {
            discount = maxDiscount;
        }

        return discount;
    }

    private void recordOperation(UUID userId, OperationType type, UUID orderId) {
        UserOperation op = UserOperation.builder()
                .userId(userId)
                .operationType(type)
                .orderId(orderId)
                .build();
        userOperationRepository.save(op);
    }

    private OrderResponse toResponse(Order order) {
        List<OrderItemResponse> items = order.getItems().stream()
                .map(item -> OrderItemResponse.builder()
                        .productId(item.getProductId())
                        .productName(item.getProductName())
                        .quantity(item.getQuantity())
                        .priceAtOrder(item.getPriceAtOrder().doubleValue())
                        .build())
                .toList();

        return OrderResponse.builder()
                .id(order.getId())
                .userId(order.getUserId())
                .status(order.getStatus().name())
                .items(items)
                .totalPrice(order.getTotalPrice().doubleValue())
                .discountedPrice(order.getDiscountedPrice() != null ? order.getDiscountedPrice().doubleValue() : null)
                .promoCodeId(order.getPromoCodeId())
                .createdAt(order.getCreatedAt())
                .updatedAt(order.getUpdatedAt())
                .build();
    }
}