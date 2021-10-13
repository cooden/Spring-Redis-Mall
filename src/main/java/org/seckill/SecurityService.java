package org.seckill;

import java.util.List;

public class SecurityService {
    public enum ProductOps {
        QUERY("query"),
        HANDLE("handle");
        private String opType;

        ProductOps(String opType) {
            this.opType = opType;
        }
    }
    public List<String> getAuthFilterProductCode(String userId, String[] products, ProductOps... opTypes){
        return getAuthProduct(userId, products, opTypes);
    }

    private List<String> getAuthProduct(String userId, String[] products, ProductOps... opTypes) {
        List<Product> authProducts = securityMapper.queryAuthProduct(userId, productList, opTypes);
        return authProducts;
    }

}
