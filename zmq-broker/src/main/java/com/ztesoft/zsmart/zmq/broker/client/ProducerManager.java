package com.ztesoft.zsmart.zmq.broker.client;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;

import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.remoting.common.RemotingHelper;
import com.ztesoft.zsmart.zmq.remoting.common.RemotingUtil;

public class ProducerManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private static final long LockTimeoutMillis = 3000;

    private static final long ChannelExpiredTimeout = 1000 * 120;

    private final Lock groupChannelLock = new ReentrantLock();

    private final HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> groupChannelTable = new HashMap<String, HashMap<Channel, ClientChannelInfo>>();

    public ProducerManager() {
    }

    public HashMap<String, HashMap<Channel, ClientChannelInfo>> getGroupChannelTable() {
        HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> newGroupChannelTable = new HashMap<String, HashMap<Channel, ClientChannelInfo>>();

        try {
            if (this.groupChannelLock.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    newGroupChannelTable.putAll(this.groupChannelTable);
                }
                finally {
                    this.groupChannelLock.unlock();
                }
            }
        }
        catch (InterruptedException e) {
            log.error("", e);
        }

        return newGroupChannelTable;
    }

    public void scanNotActiveChannel() {
        try {
            if (this.groupChannelLock.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    for (final Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                        .entrySet()) {
                        final String group = entry.getKey();
                        final HashMap<Channel, ClientChannelInfo> chlMap = entry.getValue();

                        Iterator<Entry<Channel, ClientChannelInfo>> it = chlMap.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Channel, ClientChannelInfo> item = it.next();
                            // final Integer id = item.getKey();
                            final ClientChannelInfo info = item.getValue();

                            long diff = System.currentTimeMillis() - info.getLastUpdateTimestamp();
                            if (diff > ChannelExpiredTimeout) {
                                it.remove();
                                log.warn(
                                    "SCAN: remove expired channel[{}] from ProducerManager groupChannelTable, producer group name: {}",
                                    RemotingHelper.parseChannelRemoteAddr(info.getChannel()), group);
                                RemotingUtil.closeChannel(info.getChannel());
                            }
                        }
                    }
                }
                finally {
                    this.groupChannelLock.unlock();
                }
            }
            else {
                log.warn("ProducerManager scanNotActiveChannel lock timeout");
            }
        }
        catch (InterruptedException e) {
            log.error("", e);
        }
    }

    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        if (channel != null) {
            try {
                if (this.groupChannelLock.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                    try {
                        for (Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                            .entrySet()) {
                            final String group = entry.getKey();
                            final HashMap<Channel, ClientChannelInfo> clientChannelInfoTable = entry.getValue();

                            final ClientChannelInfo clientChannelInfo = clientChannelInfoTable.remove(channel);
                            if (clientChannelInfo != null) {
                                log.info(
                                    "NETTY EVENT: remove channel[{}][{}] from ProducerManager groupChannelTable, producer group: {}",
                                    clientChannelInfo.toString(), remoteAddr, group);
                            }
                        }

                    }
                    finally {
                        this.groupChannelLock.unlock();
                    }
                }
                else {
                    log.warn("ProducerManager doChannelCloseEvent lock timeout");
                }
            }
            catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }

    public void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        try {

            ClientChannelInfo clientChannelInfoFound = null;

            if (this.groupChannelLock.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);

                    if (channelTable == null) {
                        channelTable = new HashMap<Channel, ClientChannelInfo>();
                        this.groupChannelTable.put(group, channelTable);
                    }

                    clientChannelInfoFound = channelTable.get(clientChannelInfo.getChannel());

                    if (clientChannelInfoFound == null) {
                        channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
                        log.info("new producer connected, group: {} channel: {}", group, clientChannelInfo.toString());
                    }

                }
                finally {
                    this.groupChannelLock.unlock();
                }

                if (clientChannelInfoFound != null) {
                    clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
                }
            }
            else {
                log.warn("ProducerManager registerProducer lock timeout");
            }
        }
        catch (Exception e) {
            log.error("", e);
        }
    }

    public void unregisterProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        try {
            if (this.groupChannelLock.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
                    if (null != channelTable && !channelTable.isEmpty()) {
                        ClientChannelInfo old = channelTable.remove(clientChannelInfo.getChannel());
                        if (old != null) {
                            log.info("unregister a producer[{}] from groupChannelTable {}", group,
                                clientChannelInfo.toString());
                        }

                        if (channelTable.isEmpty()) {
                            this.groupChannelTable.remove(group);
                            log.info("unregister a producer group[{}] from groupChannelTable", group);
                        }
                    }
                }
                finally {
                    this.groupChannelLock.unlock();
                }
            }
            else {
                log.warn("ProducerManager unregisterProducer lock timeout");
            }
        }
        catch (InterruptedException e) {
            log.error("", e);
        }
    }

}
