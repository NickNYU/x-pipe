package com.ctrip.xpipe.redis.proxy.monitor;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.monitor.tunnel.DefaultTunnelMonitor;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.utils.MapUtils;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Oct 29, 2018
 */
@Component
public class DefaultTunnelMonitorManager implements TunnelMonitorManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultTunnelMonitorManager.class);

    private Map<Tunnel, TunnelMonitor> tunnelMonitors = new ConcurrentHashMap<>();

    private ResourceManager resourceManager;

    public DefaultTunnelMonitorManager(ResourceManager resourceManager) {
        this.resourceManager = resourceManager;
    }

    @Override
    public TunnelMonitor getOrCreate(Tunnel tunnel) {
        return MapUtils.getOrCreate(tunnelMonitors, tunnel, new ObjectFactory<TunnelMonitor>() {
            @Override
            public TunnelMonitor create() {
                return new DefaultTunnelMonitor(resourceManager, tunnel);
            }
        });
    }

    @Override
    public void remove(Tunnel tunnel) {
        TunnelMonitor monitor = tunnelMonitors.remove(tunnel);
        try {
            if(monitor != null) {
                monitor.stop();
            }
        } catch (Exception e) {
            logger.error("[stop tunnel-monitor]", e);
        }
    }
}
