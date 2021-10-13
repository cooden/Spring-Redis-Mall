package com.webank.tdtp.pms.common.redis;

import cn.webank.jedis.Jedis;
import cn.webank.jedis.Tuple;
import cn.webank.redis.cluster.pool.ProxyJedisPool;
import com.alibaba.fastjson.JSONArray;
import com.codahale.metrics.Timer;
import com.webank.tdtp.pms.common.constant.RedisConstants;
import com.webank.tdtp.pms.common.util.CommonUtils;
import com.webank.tdtp.pms.common.util.MetricUtils;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

@Slf4j
@Component
public class RedisHashOps implements InitializingBean {
    @Override
    public void afterPropertiesSet() throws Exception {
        RedisLock.setRedisHashOps(this);
    }

    @Autowired
    private  ProxyJedisPool proxyJedisPool;

    public Jedis getJedis() {
		Jedis jedis = null;
        try {
            jedis = proxyJedisPool.getReadWriteResource();
        } catch (Exception e) {
            log.warn("redisException - Failed to get redis connection!!!", e);
            throw new RuntimeException("Failed to get redis connection!!!", e);
        }
    	return jedis;
    }

    public  void hput(String mainKey, String subKey, Object value) throws Exception {
        Jedis jedis = null;
        try {
        	mainKey = packageRedisKey(mainKey);
            jedis = getJedis();
            jedis.hset(mainKey, subKey, String.valueOf(value));
        } catch (Exception e) {
            log.warn("redisException - hset :mainKey:" + mainKey + ",subKey:"
                    + value + ",exception: " + e.getMessage(), e);
            throw e;
        }finally {
            returnResource(jedis);
        }
    }

	public  void hputAll(String mainKey, Map<String, String> value) throws Exception {
        Jedis jedis = null;
        try {
        	mainKey = packageRedisKey(mainKey);
            jedis = getJedis();
            jedis.hmset(mainKey, value);
        } catch (Exception e) {
            log.warn("redisException - hmset :mainKey:" + mainKey + ",subKey:"
                    + value + ",exception: " + e.getMessage(), e);
            throw e;
        }finally {
            returnResource(jedis);
        }
    }


    public  void hset(String mainKey, String subKey, Object value) throws Exception {
        hput(mainKey, subKey, value);
    }

    public  void safeHset(String mainKey, String subKey, Object value) {
        try {
            hput(mainKey, subKey, value);
        } catch (Exception e) {
            log.warn("redisException - hset :mainKey:" + mainKey + ",subKey:"
                    + subKey + ",exception: " + e.getMessage(), e);
        }
    }

    public  String hget(String mainKey, String subKey) {
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","hget");
        Jedis jedis = null;
        try {
        	mainKey = packageRedisKey(mainKey);
            jedis = getJedis();
            return jedis.hget(mainKey, subKey);
        } catch (Exception e) {
            log.warn("redisException - hget :mainKey:" + mainKey + ",subKey:"
                    + subKey + ",exception: " + e.getMessage(), e);
        } finally {
            returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
        return null;
    }
    
    public  boolean hputNX(String mainKey, String subKey, String value) {
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","hsetNX");
        Jedis jedis = null;
        try {
        	mainKey = packageRedisKey(mainKey);
            jedis = getJedis();
            return jedis.hsetnx(mainKey, subKey, value)!=0;
        } catch (Exception e) {
            log.warn("redisException - hsetnx :mainKey:" + mainKey + ",subKey:"
                    + subKey + ",exception: " + e.getMessage(), e);
        } finally {
            returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
        }
        return false;
    }
    

	public boolean hExists(String mainKey, String subKey) {
		Jedis jedis = null;
		try {
			jedis = getJedis();
        	mainKey = packageRedisKey(mainKey);
//			Boolean obj = redisTemplate.boundHashOps(mainKey).hasKey(subKey);
			return jedis.hexists(mainKey, subKey)==true;
		} catch (Exception e) {
			log.warn("redisException - hexists :mainKey:" + mainKey + ",subKey:" + subKey + ",exception: "
					+ e.getMessage(), e);
		} finally {
            returnResource(jedis);
		}
		return false;
	}

    public  Map<String, String> entries(String key) throws Exception {
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","entries");
        Jedis jedis = null;
        try {
        	key = packageRedisKey(key);
            jedis = getJedis();
            Map<String, String> entries = jedis.hgetAll(key);
            return entries;
        } catch (Exception e) {
            log.warn("redisException - hgetAll :key:" + key + ",exception: " + e.getMessage(), e);
            throw e;
        } finally {
            returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
    }

    public List<String> hvals(String key) throws Exception {
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","entries");
        Jedis jedis = null;
        try {
            key = packageRedisKey(key);
            jedis = getJedis();
            List<String> list = jedis.hvals(key);
            return list;
        } catch (Exception e) {
            log.warn("redisException - hvals :key:" + key + ",exception: " + e.getMessage(), e);
            throw e;
        } finally {
            returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
    }

    public  void putList(String key, BigDecimal amount) throws Exception {
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","putList");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            String stringamount = String.valueOf(amount);
            jedis.rpush(key, stringamount);
        } catch (Exception e) {
            log.warn("redisException - rpush :key:" + key + ";amount:"
                    + amount + ",exception: " + e.getMessage(), e);
            throw e;
        }finally {
            returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
    }

    public  void putListEx(String key, Object value) throws Exception {

        if (value == null) {
            return;
        }

        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","putListEx");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
        	jedis.rpush(key, value.toString());
        } catch (Exception e) {
            log.warn(String.format("redisException - rpush :key=[%s], value=[%s]", key, value.toString()), e);
            throw e;
        }finally {
            returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
    }

    public Long batchDelete(String... keys) {
        if(keys==null || keys.length==0) {
            return 0L;
        }
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","delete");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            long result = 0;
            for (String key : keys) {
                Long tmp = jedis.del(packageRedisKey(key));
                if(tmp!=null) {
                    result = result + tmp;
                }
            }
            return result;
        } catch (Exception e) {
            log.warn("redisException - del :keys:{},exception: {}", CommonUtils.parseObjectToStr(keys), e.getMessage(), e);
            throw e;
        }finally {
            returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
    }

    public  Long delete(String key) throws Exception {
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","delete");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.del(key);
        } catch (Exception e) {
            log.warn("redisException - del :key:" + key + ",exception: " + e.getMessage(), e);
            throw e;
        }finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
    }

    public  void expire(String key, long second) {
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","expire");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            jedis.expire(key, (int)second);
        } catch (Exception e) {
            log.warn("redisException - expire :key:" + key + ",exception: " + e.getMessage(), e);
        }finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
    }
    /**
     * 
     * @param key
     * @return unit(second)
     */
    public  Long ttl(String key) {
    	Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","ttl");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.ttl(key);
        } catch (Exception e) {
            log.warn("redisException - ttl :key:" + key + ",exception: " + e.getMessage(), e);
        } finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
        return 0l;
    }
    
    public  String type(String key) {
    	Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","type");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.type(key);
        } catch (Exception e) {
            log.warn("redisException - type :key:" + key + ",exception: " + e.getMessage(), e);
        } finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
        return null;
    }

    public  int set(String key, String value) {
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","set0");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            jedis.set(key, value);
            return 1;
        } catch (Exception e) {
            log.warn("redisException - set :key:" + key + ",exception: " + e.getMessage(), e);
        }finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
        return 0;
    }

    public  int set(String key, String value, long timeSecond) {
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","set1");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            jedis.psetex(key, System.currentTimeMillis()+timeSecond*1000, value);
            return 1;
        } catch (Exception e) {
            log.warn("redisException - psetex :key:" + key + ",exception: " + e.getMessage(), e);
        }finally {
            returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
        return 0;
    }

    /**
     * 当前key不存在，那么就设置key，并对key设置有效期，同时value表示加锁的客户端；
     * @param key 待加锁的key
     * @param value 标识加锁的客户端，可以用UUID表示
     * @param expireTime 过期时间
     * @return 是否成功获取锁
     */
    public boolean setNxEx(String key, String value, long expireTime) {
        final String LOCK_SUCCESS = "OK";
        final String SET_IF_NOT_EXIST = "NX";
        final String SET_WITH_EXPIRE_TIME = "PX";
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","setNX");
        String result = null;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            //1. 当前没有锁（key不存在），那么就进行加锁操作，并对锁设置个有效期，同时value表示加锁的客户端。
            //2. 已有锁存在，不做任何操作。
            result = jedis.set(key, value, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, expireTime);
        } catch (Exception e) {
            log.error(String.format("redis setNX exception: key=[%s], value=[%s], expireTime=[%s]", key, value, expireTime), e);
        } finally {
            returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }

        return LOCK_SUCCESS.equals(result);
    }

    /**
     * 删除与value对应的key
     * @param key 待删除key
     * @param value 加锁的客户端
     * @return 成功或失败
     */
    public boolean delByValue(String key, String value) {
        final Long RELEASE_SUCCESS = 1L;
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","setNX");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            String cachedValue = jedis.get(key);
            if(value.equals(cachedValue)) {
                return RELEASE_SUCCESS.equals(jedis.del(key));
            } else {
                return false;
            }
        } catch (Exception e) {
            log.error(String.format("redis delete key by value exception: key=[%s], value=[%s]", key, value), e);
        } finally {
            returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
        return true;
    }

    public  boolean setNX(String key, String value) {
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","setNX");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.setnx(key, value)!=0;
        } catch (Exception e) {
            log.warn(String.format("redisException - setnx :key=[%s], value=[%s]", key, value), e);
        } finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
        return false;
    }

    public  String get(String key) {
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","get");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.get(key);
        } catch (Exception e) {
            log.warn(String.format("redisException - get :key:[%s]", key), e);
            return null;
        } finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
    }

    /**
     * sort-set添加元素
     * 
     * @param key
     * @param score
     * @param member
     * @return
     * @author karfliu 刘锋
     * @history 2016年3月23日 新建
     */
    public  int zadd(String key, double score, String member) {

        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","zadd");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
        	Long l = jedis.zadd(key, score, member);
            return l==null ? 0 : 1;
        } catch (Exception e) {
            log.warn("redisException - zadd :key:" + key + ";score:" + score + ";member:" + member, e);
        } finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
        return 0;
    }

    public  Double zscore(String key, String member) {

        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","zscore");
        if (member == null) {
            return null;
        }
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.zscore(key, member);
        } catch (Exception e) {
            log.warn("redisException - zscore :key:" + key + ";member:" + member, e);
        } finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
        return null;
    }

    public  long zRemove(String key, String... members) {
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","zRemove");
        if (members == null || members.length == 0) {
            return 0;
        }
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            Long l = jedis.zrem(key, members);
            if (l != null) {
                return l.longValue();
            }
        } catch (Exception e) {
            log.warn("redisException - zrem :key:" + key, e);
        } finally {
        	returnResource(jedis);
        }
        return 0;
    }

    public  long zRemoveByRank(String key, int start, int end) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            Long l = jedis.zremrangeByRank(key, start, end);
            if (l != null) {
                return l.longValue();
            }
        } catch (Exception e) {
            log.warn("redisException - zremrangeByRank :key:" + key, e);
        } finally {
            returnResource(jedis);
        }
        return 0;
    }

    public  Set<Tuple> zrangeWithScores(String key, int start, int end) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.zrangeWithScores(key, start, end);
        } catch (Exception e) {
            log.warn("redisException - zrangeWithScores :key:" + key + ";start:" + start + ";end:" + end, e);
        } finally {
            returnResource(jedis);
        }
        return Collections.emptySet();
    }

    public  Set<String> zrange(String key, int start, int end) {
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","zrange");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.zrange(key, start, end);
        } catch (Exception e) {
            log.warn("redisException - zrange :key:" + key + ";start:" + start + ";end:" + end, e);
        } finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
        return Collections.emptySet();
    }

    /**
     * 根据score从sort-set中获取指定个数的元素
     * 
     * @param key
     * @param min
     * @param max
     * @param offset
     * @param count
     * @return
     * @author karfliu 刘锋
     * @history 2016年3月23日 新建
     */
    public  Set<String> rangeByScore(String key, double min, double max, long offset, long count) {

        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","rangeByScore");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.zrangeByScore(key, min, max, (int)offset, (int)count);
        } catch (Exception e) {
            log.warn("redisException - zrangeByScore :key:" + key + ";min:" + min + ";max:" + max
                    + ";offset:" + offset + ";count:" + count, e);
        } finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
        return Collections.emptySet();
    }

    public Set<String> rangeByScoreAll(String key, double min, double max) {
        return rangeByScoreAll(key, min, max, RedisConstants.BATCH_SIZE);
    }

    public Set<String> rangeByScoreAll(String key, double min, double max, int batchSize) {
        Set<String> result = new HashSet<>();
        batchSize = batchSize<=0 ? RedisConstants.BATCH_SIZE : batchSize;
        int i = 0;
        Set<String> tmpSet;
        while(true) {
            tmpSet = rangeByScore(key, min, max, i, batchSize);
            if(tmpSet!=null) {
                result.addAll(tmpSet);
            }
            if(tmpSet==null || tmpSet.size()<batchSize) {
                break;
            } else {
                i += batchSize;
            }
        }
        return result;
    }

    /**
     * 根据score从sort-set中获取指定个数的元素
     * 
     * @param key
     * @param min
     * @param max
     * @param offset
     * @param count
     * @return
     * @author karfliu 刘锋
     * @history 2016年3月23日 新建
     */
    public  Set<String> revrangeByScore(String key, double max, double min, long offset, long count) {

        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","revrangeByScore");

        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.zrevrangeByScore(key, max, min, (int)offset, (int)count);
        } catch (Exception e) {
            log.warn("redisException - zrevrangeByScore :key:" + key + ";min:" + min + ";max:"
                    + max + ";offset:" + offset + ";count:" + count, e);
        } finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
        return Collections.emptySet();
    }

    /**
     * 按指定分数段删除元素
     * 
     * @param key
     * @param min
     * @param max
     * @return
     * @author karfliu 刘锋
     * @history 2016年3月23日 新建
     */
    public  long removeRangeByScore(String key, double min, double max) {

        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","removeRangeByScore");

        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.zremrangeByScore(key, min, max);
        } catch (Exception e) {
            log.warn("redisException - zremrangeByScore :key:" + key + ";min:" + min + ";max:" + max, e);
        } finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
        return 0;
    }
    
    public double sumAmountList(String key) {
    	Timer.Context totalTimer = MetricUtils.startRedisTimer("sumAmountList","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","sumAmountList");

        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            int batchSize = RedisConstants.BATCH_SIZE;
            int startIndex = 0;
            BigDecimal total = new BigDecimal(0);
            int totalSize = 0;
            while(true) {
            	List<String> amountList = jedis.lrange(key, startIndex, startIndex+batchSize-1L);
            	if(amountList==null || amountList.isEmpty()) {
            		break;
            	} else {
            		int size = amountList.size();
            		totalSize += size;
            		for(String strAmount : amountList) {
            			total = total.add(new BigDecimal(strAmount));
            		}
            		if(size<batchSize) {
            			break;
            		} else {
            			startIndex += batchSize;
            		}
            	}
            }
            if(totalSize>0) {
            	lreduce(key, totalSize, -1, total.setScale(2, RoundingMode.HALF_UP).toString());
            }
            return total.setScale(2, RoundingMode.HALF_UP).doubleValue();
        } catch (Exception e) {
            log.warn("redisException - sumAmountList :key:{}", key, e);
        } finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
        return 0;
    }

    public  void hdel(String mainKey, String subKey) throws Exception {
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","delete");

        Jedis jedis = null;
        try {
            jedis = getJedis();
            mainKey = packageRedisKey(mainKey);
            jedis.hdel(mainKey, subKey);
        } catch (Exception e) {
            log.warn("redisException - delete :mainKey:" + mainKey
                    + ",subKey: " + subKey + ",exception: " + e.getMessage(), e);
            throw e;
        } finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
    }

    public void safeHdel(String mainKey, String subKey) {
        try {
            hdel(mainKey, subKey);
        } catch (Exception e) {
            log.warn("redisException - hdel :mainKey:" + mainKey + ",subKey:"
                    + subKey + ",exception: " + e.getMessage(), e);
        }
    }

    /*public  List<String> getList(String keys) throws Exception {

        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","BoundListOperations");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            keys = packageRedisKey(keys);
            return jedis.lrange(keys, 0, -1);
        } catch (Exception e) {
            log.warn(
                    "redisException - getList :mainKey:" + keys + ",exception: " + e.getMessage(), e);
            throw e;
        } finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
    }*/

    public List<String> lrange(String key, int start, int stop) {

        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","lrange");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.lrange(key, start, stop);
        } catch (Exception e) {
            log.warn(
                    "redisException - lrange :mainKey:" + key + ",exception: " + e.getMessage(), e);
            throw e;
        } finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
    }
    
    
    public  void lreduce(String key, int remainStart, int remainEnd, String newMember) {
    	Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","lreduce");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
        	jedis.ltrim(key, remainStart, remainEnd);
        	jedis.rpush(key, newMember);
        } catch (Exception e) {
            log.warn(
                    "redisException - getList :mainKey:" + key + ",exception: " + e.getMessage(), e);
        } finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
    }

    /**
     * @deprecated redis 3.0 not support keys
     * @param keys
     * @return
     * @throws Exception
     */
    @Deprecated
    public  Set<String> getKeys(String keys) throws Exception {
    	throw new RuntimeException("redis3.0不支持keys命令");
    }

    /**
     * sort-set批量添加元素
     * 
     * @param key
     * @return
     * @author karfliu 刘锋
     * @history 2016年3月23日 新建
     */
    public  long zadd(String key, Map<String, Double> scoreMembers) {

        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","zadd");
        if (scoreMembers == null || scoreMembers.isEmpty()) {
            return 0;
        }
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.zadd(key, scoreMembers);
        } catch (Exception e) {
            log.warn("redisException - zadd :key:" + key + ";adds:" + JSONArray.toJSONString(scoreMembers), e);
        }finally {
        	returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
        return 0;
    }

    public int sadd(String key, String member) {
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","sadd");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            Long l = jedis.sadd(key, member);
            return l==null ? 0 : 1;
        } catch (Exception e) {
            log.warn("redisException - sadd :key:" + key + ";member:" + member, e);
        } finally {
            returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
        return 0;
    }

    public Set<String> smembers(String key) {
        Timer.Context totalTimer = MetricUtils.startRedisTimer("1redis","total");
        Timer.Context timerContext = MetricUtils.startRedisTimer("redis","smembers");
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.smembers(key);
        } catch (Exception e) {
            log.warn("redisException - smembers :mainKey:" + key + ",exception: " + e.getMessage(), e);
        } finally {
            returnResource(jedis);
            MetricUtils.stopTimer(timerContext);
            MetricUtils.stopTimer(totalTimer);
        }
        return Collections.emptySet();
    }

	public  void setProxyJedisPool(ProxyJedisPool configProxyJedisPool) {
		proxyJedisPool = configProxyJedisPool;
	}
	
	
	
	public  void returnResource(Jedis jedis) {
		if(jedis!=null) {
			jedis.close();
//			proxyJedisPool.;
		}
	}
	
    /**
	 * @param mainKey
	 * @return
	 */
	private String packageRedisKey(String mainKey) {
//	    String preKey = PropertyLoad.getDynamicProp("", "3113_");
//        String preKey = PropertyLoad.getDynamicProp("redis.sysPre", "3113_");
        String sysPreKey = System.getProperty("redis.pre");
        String preKey = StringUtils.isBlank(sysPreKey) ? "3113_" : sysPreKey;
		if(!StringUtils.isBlank(mainKey)) {
			if(mainKey.startsWith(preKey)) {
				return mainKey;
			} else {
				return preKey+mainKey;
			}
		} else {
			return mainKey;
		}
	}
}
