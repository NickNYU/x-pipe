package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import org.springframework.stereotype.Component;

/**
 * @author wenchao.meng
 *         <p>
 *         May 05, 2017
 */
@Component
public class CatHealthEventProcessor implements HealthEventProcessor{

    private static final String TYPE = "HealthEvent";

    @Override
    public void onEvent(AbstractInstanceEvent instanceEvent) throws HealthEventProcessorException {
        EventMonitor.DEFAULT.logEvent(TYPE, String.format("%s-%s", instanceEvent.getClass().getSimpleName(),
                instanceEvent.getInstance().getRedisInstanceInfo().getHostPort()));
    }
}
