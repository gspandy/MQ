package com.ztesoft.zsmart.mq.zmq.srvutil;

import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * 
 * 只提供Server程序依赖，目的为了拆解客户端依赖，尽可能减少客户端的依赖 <br> 
 *  
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月21日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.mq.zmq.srvutil <br>
 */
public class ServerUtil {
    
    /**
     * 
     * 添加 command help namesrvaddr option: <br> 
     *  
     * @author wang.jun<br>
     * @taskId <br>
     * @param options
     * @return <br>
     */
    public static Options buildCommandlineOptions(final Options options){
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);
        
        opt = new Option("n", "namesrvAddr", true, 
            "Name Server Address list ,eg:10.45.0.1:9876;10.45.0.2:9876");
        opt.setRequired(true);
        options.addOption(opt);
        
        return options;
    }
    
    /**
     * 
     * 解析cmd line: <br> 
     *  
     * @author wang.jun<br>
     * @taskId <br>
     * @param appName
     * @param args
     * @param options
     * @param parser
     * @return <br>
     */
    public static CommandLine parseCmdLine(final String appName,String[] args,Options options,CommandLineParser parser){
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        CommandLine commandLine = null;
        
        try {
            commandLine = parser.parse(options, args);
            if(commandLine.hasOption("h")){
                hf.printHelp(appName, options,true);
                return null;
            }
        }
        catch (ParseException e) {
            hf.printHelp(appName, options,true);
        }
        
        return commandLine;
    }
    
    /**
     * 
     * 打印help: <br> 
     *  
     * @author wang.jun<br>
     * @taskId <br>
     * @param appName
     * @param options <br>
     */
    public static void printCommandLineHelp(final String appName,final Options options){
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        hf.printHelp(appName, options);
    }
    
    /**
     * commandline to properties
     * Description: <br> 
     *  
     * @author wang.jun<br>
     * @taskId <br>
     * @param commandLine
     * @return <br>
     */
    public static Properties commandLine2Properties(final CommandLine commandLine){
        Properties properties = new Properties();
        Option[] opts = commandLine.getOptions();
        
        if(opts != null){
            for(Option opt :opts){
                String name = opt.getLongOpt();
                String value = commandLine.getOptionValue(name);
                if(value != null){
                    properties.setProperty(name, value);
                }
            }
        }
        
        return properties;
    }
}
