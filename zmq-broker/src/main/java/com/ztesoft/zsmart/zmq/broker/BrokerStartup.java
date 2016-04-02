package com.ztesoft.zsmart.zmq.broker;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.common.BrokerConfig;
import com.ztesoft.zsmart.zmq.common.MQVersion;
import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.conflict.PackageConflictDetect;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.remoting.common.RemotingUtil;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyClientConfig;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyServerConfig;
import com.ztesoft.zsmart.zmq.remoting.netty.NettySystemConfig;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;
import com.ztesoft.zsmart.zmq.srvutil.ServerUtil;
import com.ztesoft.zsmart.zmq.store.config.BrokerRole;
import com.ztesoft.zsmart.zmq.store.config.MessageStoreConfig;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;


/**
 * broker 启动
 * 
 * @author J.Wang
 *
 */
public class BrokerStartup {
    public static Properties properties = null;
    public static CommandLine commandLine = null;
    public static String configFile = null;
    public static Logger log;


    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("port", "listenPort", true, "Name Server listen Port ");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    public static void main(String[] args) {
        start(createBrokerController(args));
    }


    /**
     * 启动broker
     * 
     * @param createBrokerController
     */
    private static void start(BrokerController controller) {
        try {
            // 启动服务控制对象
            controller.start();
            String tip = "The broker[" + controller.getBrokerConfig().getBrokerName() + ", "
                    + controller.getBrokerAddr() + "] boot success.";

            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }

            log.info(tip);
            System.out.println(tip);

            return controller;
        }
        catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }


    /**
     * 初始化BrokerController
     * 
     * @param args
     * @return
     */
    public static BrokerController createBrokerController(String[] args) {
        System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(MQVersion.CurrentVersion));

        // Socket 发送缓冲区大小 128k
        if (null == System.getProperty(NettySystemConfig.SystemPropertySocketSndbufSize)) {
            NettySystemConfig.SocketRcvbufSize = 131072;
        }
        // Socket接收缓冲区大小
        if (null == System.getProperty(NettySystemConfig.SystemPropertySocketRcvbufSize)) {
            NettySystemConfig.SocketRcvbufSize = 131072;
        }
        try {
            // 检测包冲突
            PackageConflictDetect.detectFastjson();

            // 解析命令行
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            commandLine = ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options),
                new PosixParser());

            if (commandLine == null) {
                System.exit(-1);
                return null;
            }

            // 初始化配置文件
            final BrokerConfig brokerConfig = new BrokerConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            final NettyClientConfig nettyClientConfig = new NettyClientConfig();
            nettyServerConfig.setListenPort(10911);

            if (commandLine.hasOption("port")) {
                String port = commandLine.getOptionValue("port");
                nettyServerConfig.setListenPort(Integer.parseInt(port));
            }

            final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
            if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
                int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
                messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
            }

            // 打印默认配置
            if (commandLine.hasOption("p")) {
                MixAll.printObjectProperties(null, brokerConfig);
                MixAll.printObjectProperties(null, nettyServerConfig);
                MixAll.printObjectProperties(null, nettyClientConfig);
                MixAll.printObjectProperties(null, messageStoreConfig);
                System.exit(0);
            }
            else if (commandLine.hasOption("m")) {
                MixAll.printObjectProperties(null, brokerConfig, true);
                MixAll.printObjectProperties(null, nettyServerConfig, true);
                MixAll.printObjectProperties(null, nettyClientConfig, true);
                MixAll.printObjectProperties(null, messageStoreConfig, true);
                System.exit(0);
            }

            // 指定配置文件
            if (commandLine.hasOption("c")) {
                String file = commandLine.getOptionValue("c");
                if (file != null) {
                    configFile = file;
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);

                    MixAll.properties2Object(properties, brokerConfig);
                    MixAll.properties2Object(properties, nettyServerConfig);
                    MixAll.properties2Object(properties, nettyClientConfig);
                    MixAll.properties2Object(properties, messageStoreConfig);
                    BrokerPathConfigHelper.setBrokerConfigPath(file);
                    System.out.println("load config properties file OK, " + file);
                    in.close();
                }
            }

            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);

            if (null == brokerConfig.getZmqHome()) {
                System.out.println("Please set the " + MixAll.ZMQ_HOME_ENV
                        + " variable in your environment to match the location of the RocketMQ installation");
                System.exit(-2);
            }

            // 检测Name Server地址设置是否正确IP:PORT
            String namesrvAddr = brokerConfig.getNamesrvAddr();
            if (namesrvAddr != null) {
                try {
                    String[] addrArray = namesrvAddr.split(";");
                    if (addrArray != null) {
                        for (String addr : addrArray) {
                            RemotingUtil.string2SocketAddress(addr);
                        }
                    }
                }
                catch (Exception e) {
                    System.out.printf(
                        "The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"\n",
                        namesrvAddr);
                    System.exit(-3);
                }
            }

            // BrokerId处理
            switch (messageStoreConfig.getBrokerRole()) {
            case ASYNC_MASTER:
            case SYNC_MASTER:
                // master id 必须为0
                brokerConfig.setBrokerId(MixAll.MASTER_ID);
                break;
            case SLAVE:
                if (brokerConfig.getBrokerId() <= 0) {
                    System.out.println("Slave's brokerId must be > 0");
                    System.exit(-3);
                }
                break;
            default:
                break;
            }
            // Master监听Slave请求的端口，默认为服务端口+1
            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);

            // 初始化Logback
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(brokerConfig.getZmqHome() + "/conf/logback_broker.xml");
            log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

            // 打印启动参数
            MixAll.printObjectProperties(log, brokerConfig);
            MixAll.printObjectProperties(log, nettyServerConfig);
            MixAll.printObjectProperties(log, nettyClientConfig);
            MixAll.printObjectProperties(log, messageStoreConfig);

            // 初始化服务控制对象
            final BrokerController controller = new BrokerController(//
                brokerConfig, //
                nettyServerConfig, //
                nettyClientConfig, //
                messageStoreConfig);
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;
                private AtomicInteger shutdownTimes = new AtomicInteger(0);


                @Override
                public void run() {
                    synchronized (this) {
                        log.info("shutdown hook was invoked, " + this.shutdownTimes.incrementAndGet());
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            long begineTime = System.currentTimeMillis();
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - begineTime;
                            log.info("shutdown hook over, consuming time total(ms): " + consumingTimeTotal);
                        }
                    }

                }
            }, "ShutdownHook"));

            return controller;
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

}
