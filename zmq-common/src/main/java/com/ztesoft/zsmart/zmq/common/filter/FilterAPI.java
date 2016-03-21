package com.ztesoft.zsmart.zmq.common.filter;

import java.net.URL;

import com.ztesoft.zsmart.zmq.common.protocol.heartbeat.SubscriptionData;

public class FilterAPI {
    public static String simpleClassName(final String className) {
        String simple = className;
        int index = className.lastIndexOf(".");
        if (index >= 0) {
            simple = className.substring(index + 1);
        }

        return simple;
    }

    public static URL classFile(final String className) {
        final String javaSource = simpleClassName(className) + ".java";
        URL url = FilterAPI.class.getClassLoader().getResource(javaSource);
        return url;
    }

    public static SubscriptionData buildSubscriptionData(final String consumerGroup, String topic, String subString)
        throws Exception {
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);

        if (null == subString || subString.equals(SubscriptionData.SUB_ALL) || subString.length() == 0) {
            subscriptionData.setSubString(SubscriptionData.SUB_ALL);
        }
        else {
            String[] tags = subString.split("\\|\\|");
            if (tags != null && tags.length > 0) {
                for (String tag : tags) {
                    if (tag.length() > 0) {
                        String trimString = tag.trim();
                        if (trimString.length() > 0) {
                            subscriptionData.getTagsSet().add(trimString);
                            subscriptionData.getCodeSet().add(trimString.hashCode());
                        }
                    }
                }
            }
            else {
                throw new Exception("subString split error");
            }
        }

        return subscriptionData;
    }
}
