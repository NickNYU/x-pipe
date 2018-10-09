package com.ctrip.xpipe.redis.console.healthcheck.redisconf.version;

import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.redisconf.AbstractCDLAHealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.redisconf.CrossDcLeaderAwareHealthCheckAction;
import org.springframework.stereotype.Component;

/**
 * @author chen.zhu
 * <p>
 * Oct 08, 2018
 */
@Component
public class VersionCheckActionFactory extends AbstractCDLAHealthCheckActionFactory {

    @Override
    public CrossDcLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
        return new VersionCheckAction(scheduled, instance, executors, alertManager);
    }

    @Override
    public Class<? extends CrossDcLeaderAwareHealthCheckAction> support() {
        return VersionCheckAction.class;
    }
}
