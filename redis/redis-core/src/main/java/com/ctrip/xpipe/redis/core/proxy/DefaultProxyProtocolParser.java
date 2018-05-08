package com.ctrip.xpipe.redis.core.proxy;

import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyOptionParser;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;

import java.util.List;

import static com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser.LINE_SPLITTER;
import static com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser.WHITE_SPACE;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public class DefaultProxyProtocolParser implements ProxyProtocolParser {

    private static final String KEY_WORD = "Proxy";

    private List<ProxyOptionParser> parsers = Lists.newArrayList();

    public void addProxyParser(ProxyOptionParser parser) {
        parsers.add(parser);
    }

    @Override
    public ProxyOptionParser getProxyOptionParser(PROXY_OPTION proxyOption) {
        for(ProxyOptionParser parser : parsers) {
            if(parser.option() == proxyOption) {
                return parser;
            }
        }
        ProxyOptionParser parser = proxyOption.getProxyOptionParser();
        addProxyParser(parser);
        return parser;
    }

    @Override
    public ByteBuf format() {
        StringBuilder proxyProtocol = new StringBuilder(KEY_WORD).append(WHITE_SPACE);
        for(ProxyOptionParser parser : parsers) {
            proxyProtocol.append(parser.getPayload()).append(";");
        }
        return new SimpleStringParser(proxyProtocol.toString()).format();
    }

    @Override
    public ProxyProtocol read(String protocol) {
        protocol = removeKeyWord(protocol);
        String[] allOption = protocol.split(LINE_SPLITTER);
        for(String option : allOption) {
            addProxyParser(PROXY_OPTION.parse(option.trim()));
        }
        return new DefaultProxyProtocol(this);
    }

    private String removeKeyWord(String protocol) {
        if(protocol.toLowerCase().contains(KEY_WORD.toLowerCase())) {
            return protocol.substring(KEY_WORD.length());
        }
        return protocol;
    }
}
