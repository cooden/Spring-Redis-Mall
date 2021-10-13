package org.seckill;

import com.codahale.metrics.MetricRegistry;
import org.aspectj.lang.annotation.Aspect;

@Aspect
public class performance {
    static final MetricRegistry metrics = new MetricRegistry();
}
