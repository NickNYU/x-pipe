package com.ctrip.xpipe.redis.console.controller.api.migrate;

import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.CheckPrepareRequest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.CheckPrepareResponse;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.DoRequest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.DoResponse;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.utils.OsUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.junit.Test;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.nio.channels.Channel;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

public class MigrationApiTest {

    private String prefix = "http://xpipe.uat.qa.nt.ctripcorp.com/api/migration";



    @Test
    public void manulConcurrentTest() throws InterruptedException {
        String clusterPrefix = "xpipe-auto-build-";
        List<Long> tickets = Lists.newArrayList();
        for (int i = 50; i < 93; i ++) {
            if (i == 62) continue;
            if (i == 80) continue;
            tickets.add(checkAndPrepare(clusterPrefix + i));
        }
        ExecutorService executors = Executors.newFixedThreadPool(OsUtils.getCpuCount());
        List<DoRequest> requests = doMigrate(tickets);
        RestTemplate restTemplate = RestTemplateFactory.createRestTemplate();
        String url = String.format("%s/%s", prefix, "domigration");
        for (DoRequest request : requests) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    restTemplate.postForEntity(url, request, DoResponse.class);
                }
            });
        }
        Thread.sleep(100000);
    }

    private long checkAndPrepare(String cluster) {
        String url = String.format("%s/%s", prefix, "checkandprepare");
        CheckPrepareRequest request = new CheckPrepareRequest();
        request.setFromIdc("UAT");
        request.setToIdc("NTGXH");
//        request.setFromIdc("NTGXH");
//        request.setToIdc("UAT");
        request.setClusters(Lists.newArrayList(cluster));
        RestTemplate restTemplate = RestTemplateFactory.createRestTemplate();
        ResponseEntity<CheckPrepareResponse> response =  restTemplate.postForEntity(url, request, CheckPrepareResponse.class);
        return response.getBody().getTicketId();
    }

    private List<DoRequest> doMigrate(List<Long> tickets) {

        List<DoRequest> result = Lists.newArrayList();
        for (long ticketId : tickets) {
            DoRequest doRequest = new DoRequest();
            doRequest.setTicketId(ticketId);
            result.add(doRequest);
        }
        return result;
    }
}