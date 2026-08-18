package com.example.cqrsproductqueryservice.controller;

import com.example.cqrsproductqueryservice.entity.Product;
import com.example.cqrsproductqueryservice.service.ProductQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/products")
public class ProductQueryController {
    @Autowired
    private ProductQueryService productQueryService;

    @GetMapping
    public List<Product> fetchAllProducts() {
        return productQueryService.getProducts();
    }
}
