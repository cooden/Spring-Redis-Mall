package org.seckill.job;

import java.util.concurrent.atomic.AtomicInteger;

public class ReportJob extends BaseJob {

    private AtomicInteger logCount = new AtomicInteger(0);

}
