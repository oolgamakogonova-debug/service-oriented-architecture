package com.example.shop.service;

import com.example.shop.dto.PageResponse;
import com.example.shop.dto.product.*;
import com.example.shop.entity.Product;
import com.example.shop.entity.enums.ProductStatus;
import com.example.shop.entity.enums.UserRole;
import com.example.shop.exception.AccessDeniedException;
import com.example.shop.exception.ProductNotFoundException;
import com.example.shop.repository.ProductRepository;
import com.example.shop.security.UserPrincipal;
import jakarta.persistence.criteria.Predicate;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;

    public PageResponse<ProductResponse> getProducts(int page, int size, String status,
                                                      String category, Double minPrice,
                                                      Double maxPrice, String sort, String direction) {
        Sort.Direction dir = "ASC".equalsIgnoreCase(direction) ? Sort.Direction.ASC : Sort.Direction.DESC;
        PageRequest pageable = PageRequest.of(page, size, Sort.by(dir, sort));

        Specification<Product> spec = (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (status != null && !status.isBlank()) {
                predicates.add(cb.equal(root.get("status"), ProductStatus.valueOf(status)));
            }
            if (category != null && !category.isBlank()) {
                predicates.add(cb.equal(root.get("category"), category));
            }
            if (minPrice != null) {
                predicates.add(cb.greaterThanOrEqualTo(root.get("price"), BigDecimal.valueOf(minPrice)));
            }
            if (maxPrice != null) {
                predicates.add(cb.lessThanOrEqualTo(root.get("price"), BigDecimal.valueOf(maxPrice)));
            }

            return cb.and(predicates.toArray(new Predicate[0]));
        };

        Page<Product> result = productRepository.findAll(spec, pageable);

        List<ProductResponse> content = result.getContent().stream()
                .map(this::toResponse)
                .toList();

        return PageResponse.<ProductResponse>builder()
                .content(content)
                .page(result.getNumber())
                .size(result.getSize())
                .totalElements(result.getTotalElements())
                .totalPages(result.getTotalPages())
                .build();
    }

    public ProductResponse getById(UUID id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new ProductNotFoundException(id));
        return toResponse(product);
    }

    @Transactional
    public ProductResponse create(ProductCreateRequest request, UserPrincipal principal) {
        if (principal.getRole() != UserRole.SELLER && principal.getRole() != UserRole.ADMIN) {
            throw new AccessDeniedException("Only SELLER or ADMIN can create products");
        }

        Product product = Product.builder()
                .name(request.getName())
                .description(request.getDescription())
                .price(BigDecimal.valueOf(request.getPrice()))
                .stock(request.getStock())
                .category(request.getCategory())
                .status(ProductStatus.valueOf(request.getStatus()))
                .sellerId(principal.getId())
                .build();

        product = productRepository.save(product);
        return toResponse(product);
    }

    @Transactional
    public ProductResponse update(UUID id, ProductUpdateRequest request, UserPrincipal principal) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new ProductNotFoundException(id));

        checkOwnership(product, principal);

        product.setName(request.getName());
        product.setDescription(request.getDescription());
        product.setPrice(BigDecimal.valueOf(request.getPrice()));
        product.setStock(request.getStock());
        product.setCategory(request.getCategory());
        product.setStatus(ProductStatus.valueOf(request.getStatus()));

        product = productRepository.save(product);
        return toResponse(product);
    }

    @Transactional
    public void delete(UUID id, UserPrincipal principal) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new ProductNotFoundException(id));

        checkOwnership(product, principal);

        product.setStatus(ProductStatus.ARCHIVED);
        productRepository.save(product);
    }

    private void checkOwnership(Product product, UserPrincipal principal) {
        if (principal.getRole() == UserRole.ADMIN) {
            return; // Admin can manage all
        }
        if (principal.getRole() != UserRole.SELLER) {
            throw new AccessDeniedException("Only SELLER or ADMIN can manage products");
        }
        if (!product.getSellerId().equals(principal.getId())) {
            throw new AccessDeniedException("You can only manage your own products");
        }
    }

    private ProductResponse toResponse(Product p) {
        return ProductResponse.builder()
                .id(p.getId())
                .name(p.getName())
                .description(p.getDescription())
                .price(p.getPrice().doubleValue())
                .stock(p.getStock())
                .category(p.getCategory())
                .status(p.getStatus().name())
                .sellerId(p.getSellerId())
                .createdAt(p.getCreatedAt())
                .updatedAt(p.getUpdatedAt())
                .build();
    }
}