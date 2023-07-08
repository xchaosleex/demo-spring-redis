package com.tugay.pubsub.service;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.connection.Message;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tugay.pubsub.model.Product;
import com.tugay.pubsub.model.ProductRequest;
import com.tugay.pubsub.repository.ProductRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProductService {

    private final RedissonClient redisson;
    private final ProductRepository productRepository;
    private final ObjectMapper mapper = new ObjectMapper();

    @Retryable(value = org.redisson.client.RedisTimeoutException.class, maxAttempts = 2, backoff = @Backoff(delay = 2000))
    public void updateStock(final Message message) {
        RLock lock = redisson.getLock("update-stock-lock");
        try {
            lock.lock(4, TimeUnit.SECONDS);
            log.info("RedissonClient Lock By : Subscriber ONE");
            String body = new String(message.getBody(), StandardCharsets.UTF_8);
            ProductRequest requestProduct = null;
            try {
                requestProduct = mapper.readValue(body, ProductRequest.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            assert requestProduct != null;
            List<Product> products = productRepository.findByProductName(requestProduct.getProductName());
            if (products!=null) {
                products.sort((o1, o2) -> o2.getId().compareTo(o1.getId()));
                Product product = products.get(0);
                if (product.getStock() > 0) {
                    productRepository.save(Product.builder().productName(product.getProductName())
                            .lastUpdateBy("SUBSCRIBER ONE")
                            .stock(product.getStock() - 1).build());
                }

            }
        } finally {
            if (lock != null && lock.isLocked() && lock.isHeldByCurrentThread()) {
                lock.unlock();
                log.info("lock released by ONE");
            }
        }
    }

}
