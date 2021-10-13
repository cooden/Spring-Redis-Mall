package org.seckill;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Tuple;
import com.alibaba.fastjson.JSONArray;
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
    private  JedisPool jedisPool;

    public Jedis getJedis() {
		Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
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
        }
        return null;
    }

    public  boolean hputNX(String mainKey, String subKey, String value) {
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
        }
    }

    public List<String> hvals(String key) throws Exception {
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
        }
    }

    public  void putList(String key, BigDecimal amount) throws Exception {
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
        }
    }

    public  void putListEx(String key, Object value) throws Exception {

        if (value == null) {
            return;
        }

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
        }
    }

    public Long batchDelete(String... keys) throws Exception {
        if(keys==null || keys.length==0) {
            return 0L;
        }
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
            log.warn("redisException - del :keys:{},exception: {}", keys.toString(), e.getMessage(), e);
            throw e;
        }finally {
            returnResource(jedis);
        }
    }

    public  Long delete(String key) throws Exception {
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
        }
    }

    public  void expire(String key, long second) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            jedis.expire(key, (int)second);
        } catch (Exception e) {
            log.warn("redisException - expire :key:" + key + ",exception: " + e.getMessage(), e);
        }finally {
        	returnResource(jedis);
        }
    }
    /**
     *
     * @param key
     * @return unit(second)
     */
    public  Long ttl(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.ttl(key);
        } catch (Exception e) {
            log.warn("redisException - ttl :key:" + key + ",exception: " + e.getMessage(), e);
        } finally {
        	returnResource(jedis);
        }
        return 0l;
    }

    public  String type(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.type(key);
        } catch (Exception e) {
            log.warn("redisException - type :key:" + key + ",exception: " + e.getMessage(), e);
        } finally {
        	returnResource(jedis);
        }
        return null;
    }

    /**
     * 注意：此方法是没有设置缓存过期时间的。必须在缓存数据很长久使用的情况下才能调用此方法。
     *
     * @param key
     * @param value
     * @return
     */
    public  int set(String key, String value) {
        // 注意：此方法是没有设置缓存过期时间的。必须在缓存数据很长久使用的情况下才能调用此方法。
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
        }
        return 0;
    }

    public  int set(String key, String value, long timeSecond) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            jedis.psetex(key, timeSecond*1000, value);
            return 1;
        } catch (Exception e) {
            log.warn("redisException - psetex :key:" + key + ",exception: " + e.getMessage(), e);
        }finally {
            returnResource(jedis);
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
        }
        return true;
    }

    public  boolean setNX(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.setnx(key, value)!=0;
        } catch (Exception e) {
            log.warn(String.format("redisException - setnx :key=[%s], value=[%s]", key, value), e);
        } finally {
        	returnResource(jedis);
        }
        return false;
    }

    public  String get(String key) {
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
        }
        return 0;
    }

    public  Double zscore(String key, String member) {

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
        }
        return null;
    }

    public  long zRemove(String key, String... members) {
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
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.zrange(key, start, end);
        } catch (Exception e) {
            log.warn("redisException - zrange :key:" + key + ";start:" + start + ";end:" + end, e);
        } finally {
        	returnResource(jedis);
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
    public  Set<String> revrangeByScore(String key, double max, double min, long offset, long count) {


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


        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.zremrangeByScore(key, min, max);
        } catch (Exception e) {
            log.warn("redisException - zremrangeByScore :key:" + key + ";min:" + min + ";max:" + max, e);
        } finally {
        	returnResource(jedis);
        }
        return 0;
    }


    public  void hdel(String mainKey, String subKey) throws Exception {

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


        }
    }*/

    public List<String> lrange(String key, int start, int stop) throws Exception {

        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.lrange(key, start, stop);
        } catch (Exception e) {
            log.warn("redisException - lrange :mainKey:" + key + ",exception: " + e.getMessage(), e);
            throw e;
        } finally {
        	returnResource(jedis);
        }
    }


    public  void lreduce(String key, int remainStart, int remainEnd, String newMember) {
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
        }
        return 0;
    }

    public int sadd(String key, String member) {
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
        }
        return 0;
    }

    public Set<String> smembers(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            key = packageRedisKey(key);
            return jedis.smembers(key);
        } catch (Exception e) {
            log.warn("redisException - smembers :mainKey:" + key + ",exception: " + e.getMessage(), e);
        } finally {
            returnResource(jedis);
        }
        return Collections.emptySet();
    }

	public  void setJedisPool(JedisPool configJedisPool) {
		jedisPool = configJedisPool;
	}



	public  void returnResource(Jedis jedis) {
		if(jedis!=null) {
			jedis.close();
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
