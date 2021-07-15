package com.uc.db.config;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wangxiaobo on 2018/3/12.
 */
public class UcDataSourceBaseConfig {
    private static final Logger logger = LoggerFactory.getLogger(UcDataSourceBaseConfig.class);

    private String configFile;

    private Properties[] baseConfigProperties;

    public Properties[] getBaseConfigProperties() {
        return baseConfigProperties;
    }

    public UcDataSourceBaseConfig(String configFile){
        this.configFile = configFile;
    }

    public void init() throws IOException {
        Resource resource = null;
        if (configFile.startsWith("file:") == true){
            String tempFile = configFile.replace("file:","");
            resource = new FileSystemResource(tempFile);
        }else if (configFile.startsWith("classpath:") == true){
            String tempFile = configFile.replace("classpath:","");
            resource = new ClassPathResource(tempFile);
        }else{
            String tempFile = configFile.replace("classpath:","");
            resource = new ClassPathResource(tempFile);
        }

        TreeMap<Integer,Properties> map = Maps.newTreeMap();
        Properties properties = PropertiesLoaderUtils.loadProperties(resource);
        properties.forEach((oKey,oValue) -> {
            String sKey = oKey.toString();
            String sRex = "^database\\[(0|[1-9]\\d*)\\]\\.";
            Pattern pattern = Pattern.compile(sRex);
            Matcher matcher = pattern.matcher(sKey);
            boolean bFind = matcher.find();
            if (bFind == false)
                throw  new RuntimeException("读取配置文件行错误:" + sKey);
            String sFind = matcher.group(0);
            int nTemp = Integer.parseInt(matcher.group(1));
            Properties p = null;
            if (map.containsKey(nTemp) == false){
                p = new Properties();
                map.put(nTemp,p);
            }else {
                p = map.get(nTemp);
            }
            String sName = sKey.substring(sFind.length());
            String sValue = oValue.toString();
            p.put(sName,sValue);
        });

        int nCount = map.size();
        baseConfigProperties = new Properties[nCount];

        map.forEach((oKey,oValue)->{
            baseConfigProperties[oKey] = oValue;
        });
    }
}
