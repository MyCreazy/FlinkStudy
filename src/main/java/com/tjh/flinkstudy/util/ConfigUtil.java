package com.tjh.flinkstudy.util;


import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * Created by tjh.
 * Date: 2019/4/10 下午3:51
 **/
public class ConfigUtil {
    private static String environmentConfig = "";

    static {
        String propertyName = "systemconfig";
        // 获得资源包
        ResourceBundle rb = ResourceBundle.getBundle(propertyName.trim(), new Locale("zh", "CN"));
        // 通过资源包拿到所有的key
        Enumeration<String> allKey = rb.getKeys();
        while (allKey.hasMoreElements()) {
            String tempkey = null;
            try {
                tempkey = new String(allKey.nextElement().getBytes("ISO-8859-1"), "GBK");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            String value = (String) rb.getString(tempkey);
            if (tempkey.equals("config.version")) {
                environmentConfig = value;
                break;
            }
        }
    }


    public static Map<String, String> getAllMessage(String propertyPrefix) throws UnsupportedEncodingException {
        Map<String, String> configMap = new HashMap<>();
        String propertyName = "eventtrigger";
        if (!StringUtils.isEmpty(environmentConfig)) {
            propertyName = propertyName + environmentConfig;
        }
        // 获得资源包
        ResourceBundle rb = ResourceBundle.getBundle(propertyName.trim(), new Locale("zh", "CN"));
        // 通过资源包拿到所有的key
        Enumeration<String> allKey = rb.getKeys();
        while (allKey.hasMoreElements()) {
            String tempkey = new String(allKey.nextElement().getBytes("ISO-8859-1"), "GBK");
            String[] keyArray = tempkey.split("\\.");
            if (keyArray != null && keyArray.length > 0) {
                List<String> tempkeyList = Arrays.asList(keyArray);
                List<String> keyList = new ArrayList<>(tempkeyList);
                String tempPrefix = keyList.get(0);
                if (tempPrefix.equals(propertyPrefix)) {
                    keyList.remove(tempPrefix);
                    String value = (String) rb.getString(tempkey);

                    String key = String.join(".", keyList);
                    configMap.put(key, value);
                }
            }
        }
        return configMap;
    }
}
