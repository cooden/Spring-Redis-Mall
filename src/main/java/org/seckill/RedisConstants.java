package org.seckill;

public class RedisConstants {
    public static final String KEY_APP_LOCK_MAIN = "lock_main";
    public static final String APPEND_KEY_SPLIT = ":";

    public enum App{
        cooden("coo", "管理系统");

        private String key;
        private String name;

        App(String coo, String 管理系统) {


        }

        public String getAppKey() {
            return key;
        }
    }
}
