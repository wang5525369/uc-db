package com.uc.db.config;

import com.uc.db.constant.UcDbConstants;
import com.uc.db.annotation.UcEnableDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;
import java.util.Set;

/**
 * Created by wangxiaobo on 2018/9/6.
 */
@ConditionalOnBean(annotation = {UcEnableDataSource.class})
@Configuration
public class UcDataSourceConfig {
    private static final Logger logger = LoggerFactory.getLogger(UcDataSourceConfig.class);

    @Resource(name = "ucDruidDataSourceExternalConfig")
    Properties ucDruidDataSourceExternalConfig;
    @Autowired
    UcDataSourceBaseConfig ucDataSourceBaseConfig;

    public Properties convertConfigPropety(Properties configPropety){
        Properties properties = new Properties();
        String dbType = configPropety.getProperty("type");
        dbType = dbType.toLowerCase();

        if (UcDbConstants.DURID_TYPE_NAME.equals(dbType) == true) {
            properties.putAll(ucDruidDataSourceExternalConfig);
            properties.putAll(configPropety);
            properties = convertDruidConfigPropety(properties);

        }else{
            properties = configPropety;
        }
        return properties;
    }

    private Properties convertDruidConfigPropety(Properties druidConfigPropety){
        Properties properties = new Properties();
        Set<String> arrayName = druidConfigPropety.stringPropertyNames();
        for(String name : arrayName){
            String sValue = druidConfigPropety.getProperty(name);
            String druidName = "druid." + name;
            properties.put(druidName,sValue);
        }
        return properties;
    }

    static public Properties buildDruidProperties(String propertiesFromString, String entrySeparator) throws IOException {
        Properties properties = new Properties();
        if (StringUtils.isBlank(propertiesFromString) == false) {
            properties.load(new StringReader(propertiesFromString.replaceAll(entrySeparator, "\n")));
        }
        return properties;
    }

    public Properties[] getBaseConfigPropetys(){
        Properties[] baseConfigProperties = ucDataSourceBaseConfig.getBaseConfigProperties();
        return baseConfigProperties;
    }


}
