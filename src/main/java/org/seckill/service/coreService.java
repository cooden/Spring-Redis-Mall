package org.seckill.service;

import org.seckill.RedisHashOps;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

public class coreService {

    @Autowired
    RedisHashOps redisHashOps;

    private void repairNetDebitLimitRecord(){
        String redisCache = redisHashOps.hget();
    }

    @SuppressWarnings("unchecked")
    private Map<String, List<Object>> getUnsettledRoutesAmt() {
        return null;
    }
}
