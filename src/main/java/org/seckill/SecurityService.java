package org.seckill;

import org.seckill.dao.SecurityDao;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class SecurityService {

    @Autowired
    SecurityDao securityDao;

    public enum ProductOps {
        QUERY("query"),
        HANDLE("handle");
        private String opType;

        ProductOps(String opType) {
            this.opType = opType;
        }
    }
    public List<Product> getAuthFilterProductCode(String userId, String[] products, ProductOps... opTypes){
        return getAuthProduct(userId, products, opTypes);
    }

    //可变长参数
    private List<Product> getAuthProduct(String userId, String[] products, ProductOps... opTypes) {
        List<Product> authProducts = securityDao.queryAuthProduct(userId, opTypes);
        return authProducts;
    }

}
