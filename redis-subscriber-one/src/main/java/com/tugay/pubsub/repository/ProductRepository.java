package com.tugay.pubsub.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.tugay.pubsub.model.Product;

@Repository
public interface ProductRepository extends JpaRepository<Product, Long> {

	List<Product> findByProductName(String productName);
}

