/**
 * 
 * FileName: RedisLock.java
 * @author   karfliu
 * @Date     2018年4月17日
 * @version  2.0.0
 * 
 */
package org.seckill;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.Random;
import org.seckill.RedisConstants.App;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * 
 * FileName: RedisLock.java
 * 
 */
@Slf4j
public class RedisLock {
	private static RedisHashOps redisHashOps;
	/**
	 * 锁最多持有5分钟
	 */
	public static final long MAX_LOCK_TIME = 300;
	/**
	 * key不存在时，ttl值为-2
	 */
	public static final long TTL_NOT_EXISTS = -2;
	/**
	 * @param redisHashOps the redisHashOps to set
	 */
	public static void setRedisHashOps(RedisHashOps redisHashOps) {
		if(RedisLock.redisHashOps==null)
			RedisLock.redisHashOps = redisHashOps;
	}

	private static String localIp;
    private static int localPid;
    static {
        if (StringUtils.isEmpty(localIp)) {
            synchronized (RedisLock.class) {
                if (StringUtils.isEmpty(localIp)) {
                    localPid = new Random().nextInt(10000);
                    localIp = "127.0.0.1";
                    String name = ManagementFactory.getRuntimeMXBean().getName();
                    if (!StringUtils.isEmpty(name) && name.indexOf("@") > -1) {
                        localPid = Integer.parseInt(name.split("@")[0]);
                    }
                    try {
                        InetAddress address = InetAddress.getLocalHost();
                        localIp = address.getHostAddress();
                    } catch (Exception e) {
                        log.error( "无法获取当前机器IP， 使用127.0.0.1默认IP替代。", e);
                    }
                }
            }
        }
    }
    
	private static String getServerAppTag(String dcn) {
		return dcn + "||" + localIp + "||" + localPid;
	}

	/**
	 * 获取应用全局锁
	 * @param app
	 * @param dcn
	 * @param lockSecond
	 * @return
	 */
	public static boolean lockApp(App app, String dcn, long lockSecond) {
    	boolean ignoreAppLock = "true".equals(System.getProperty("ignoreAppLock"));
    	// 支持忽略应用锁参数
    	return ignoreAppLock || lockApp1(app, dcn, lockSecond);
	}

	/**
	 * 释放当前占用的应用全局锁
	 * @return
	 */
	public static boolean unlockApp(App app, String dcn) {
		return lockApp1(app, dcn, 3L);
	}
    
    private static boolean lockApp1(App app, String dcn, long lockSecond) {
    	if(app==null) {
    		return false;
    	} else {
    		int i = 0;
    		while(i++<3 && redisHashOps == null) {
				try {
					Thread.sleep(3000L);
				} catch (Exception e) {
				}
			}
    		String cacheKey = RedisConstants.KEY_APP_LOCK_MAIN+RedisConstants.APPEND_KEY_SPLIT+app.getAppKey();
    		String serverTag = getServerAppTag(dcn);
    		Long ttl = redisHashOps.ttl(cacheKey);
    		boolean success = false;
    		if(ttl==null || ttl==-2) {
				success = redisHashOps.setNX(cacheKey, serverTag);
			} else if(ttl<0 || ttl>MAX_LOCK_TIME) {
				redisHashOps.expire(cacheKey, MAX_LOCK_TIME);
			}
        	if(!success) {
        		String cacheServerTag = redisHashOps.get(cacheKey);
        		if(serverTag.equals(cacheServerTag)) {
        			success = true;
        			if(ttl<lockSecond) {
						redisHashOps.expire(cacheKey, lockSecond);
					}
        		} else {
        			success = false;
        		}
        	} else {
    			redisHashOps.expire(cacheKey, lockSecond);
        	}
        	return success;
    	}
    }

}
