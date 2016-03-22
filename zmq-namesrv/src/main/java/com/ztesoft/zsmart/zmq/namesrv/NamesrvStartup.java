package com.ztesoft.zsmart.zmq.namesrv;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;

import com.ztesoft.zsmart.zmq.common.MQVersion;
import com.ztesoft.zsmart.zmq.common.MixAll;
import com.ztesoft.zsmart.zmq.common.conflict.PackageConflictDetect;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.common.namesrv.NamesrvConfig;
import com.ztesoft.zsmart.zmq.common.utils.StringUtils;
import com.ztesoft.zsmart.zmq.remoting.netty.NettyServerConfig;
import com.ztesoft.zsmart.zmq.remoting.netty.NettySystemConfig;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;
import com.ztesoft.zsmart.zmq.srvutil.ServerUtil;

/**
 * name server 启动入口 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月22日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.namesrv <br>
 */
public class NamesrvStartup {
    public static Properties properties = null;

    public static CommandLine commandLine = null;

    /**
     * 设置configFile 和 printConfigItem 命令: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param options
     * @return <br>
     */
    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("port", "listenPort", true, "Name Server listen Port ");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static void main(String[] args) {
        main0(args);
    }

    public static NamesrvController main0(String[] args) {

        System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(MQVersion.CurrentVersion));

        // 设置Socket发送缓冲区大小
        if (null == System.getProperty(NettySystemConfig.SystemPropertySocketSndbufSize)) {
            NettySystemConfig.SocketSndbufSize = 2048;
        }

        // 设置Socket接收缓冲区大小
        if (null == System.getProperty(NettySystemConfig.SystemPropertySocketRcvbufSize)) {
            NettySystemConfig.SocketRcvbufSize = 2048;
        }
        try {
            // 检测fastjson 包冲突
            PackageConflictDetect.detectFastjson();
            // 解析命令行
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            commandLine = ServerUtil.parseCmdLine("namesrv", args, buildCommandlineOptions(options), new PosixParser());

            if (null == commandLine) {
                System.exit(-1);
                return null;
            }

            // 初始化配置文件
            final NamesrvConfig namesrvConfig = new NamesrvConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();

            // 设置服务端监听端口9876
            nettyServerConfig.setListenPort(9876);

            if (commandLine.hasOption("port")) {
                String port = commandLine.getOptionValue("port");
                if(StringUtils.isNumber(port)){
                    nettyServerConfig.setListenPort(Integer.parseInt(port));
                }
            }

            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);
                    MixAll.properties2Object(properties, nettyServerConfig);
                    MixAll.properties2Object(properties, namesrvConfig);
                    System.out.println("load config properties file OK, " + file);
                    in.close();
                }
            }

            // 打印默认配置
            if (commandLine.hasOption('p')) {
                MixAll.printObjectProperties(null, namesrvConfig);
                MixAll.printObjectProperties(null, nettyServerConfig);
                System.exit(0);
            }

            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);

            if (null == namesrvConfig.getZmqHome()) {
                System.out.println("Please set the " + MixAll.ZMQ_HOME_ENV
                    + " variable in your environment to match the location of the ZMQ installation");
                System.exit(-2);
            }
            // 初始化logback
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(namesrvConfig.getZmqHome() + "/conf/logback_namesrv.xml");
            final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);

            // 打印服务配置参数
            MixAll.printObjectProperties(log, namesrvConfig);
            MixAll.printObjectProperties(log, nettyServerConfig);

            // 初始化服务控制对象
            final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);

            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            // 添加守护线程
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

                private volatile boolean hasShutdown = false;

                private AtomicLong shutdownTimes = new AtomicLong(0);

                @Override
                public void run() {
                    synchronized (this) {
                        log.info("shutdown hook was invoked ,_" + this.shutdownTimes.incrementAndGet());

                        if (!hasShutdown) {
                            this.hasShutdown = true;
                            long begineTime = System.currentTimeMillis();
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - begineTime;
                            log.info("shutdown hook over, consuming time total(ms): " + consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));

            // 启动服务
            controller.start();

            String tip = "The Name Server boot success.";
            log.info(tip);
            System.out.println(tip);

            return controller;

        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }
}
