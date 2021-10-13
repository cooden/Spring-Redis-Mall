package org.seckill.job;

public class BaseJob {

    protected boolean getAppLock(){
        long curSecond = System.currentTimeMillis();
        return true;
    }

}
