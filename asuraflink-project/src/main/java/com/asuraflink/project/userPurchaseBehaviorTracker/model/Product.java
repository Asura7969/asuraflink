package com.asuraflink.project.userPurchaseBehaviorTracker.model;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Product {
    private Integer productId;
    private double price;
    private Integer amount;
}
