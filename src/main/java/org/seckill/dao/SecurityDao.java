package org.seckill.dao;

import org.seckill.Product;
import org.seckill.SecurityService.ProductOps;

import java.util.List;

public interface SecurityDao {

    List<Product> queryAuthProduct(String userId, ProductOps[] opType);
}
