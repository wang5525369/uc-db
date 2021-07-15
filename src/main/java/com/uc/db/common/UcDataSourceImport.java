package com.uc.db.common;

import com.google.common.collect.Lists;
import com.uc.db.config.UcDruidWebConfiguration;
import com.uc.db.datasource.UcSingleDataSource;
import com.uc.db.annotation.UcEnableDataSource;
import com.uc.db.datasource.UcMultiDataSource;
import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.StandardAnnotationMetadata;

import java.util.List;
import java.util.Map;

public class UcDataSourceImport implements ImportSelector {

    static String dbConfigFilePathName = "";
    static String dbMapperPath = "";
    static boolean dbMultiDataSource = false;
    static boolean dbShowWeb = false;

    @Override
    public String[] selectImports(AnnotationMetadata importingClassMetadata) {
        String [] arrayBeanName = new String[0];
        if (importingClassMetadata instanceof StandardAnnotationMetadata){
            StandardAnnotationMetadata standardAnnotationMetadata = ((StandardAnnotationMetadata) importingClassMetadata);
            String name = UcEnableDataSource.class.getName();
            Map<String,Object> mapAttribute = standardAnnotationMetadata.getAnnotationAttributes(name);
            if (mapAttribute.size() > 0) {

                List<String> listBeanName = Lists.newArrayList();

                dbConfigFilePathName = (String) mapAttribute.get("configFile");
                dbMapperPath = (String) mapAttribute.get("mapperPath");

                dbShowWeb = (boolean) mapAttribute.get("isShowWeb");
                if (dbShowWeb == true) {
                    listBeanName.add(UcDruidWebConfiguration.class.getName());
                }

                dbMultiDataSource = (boolean) mapAttribute.get("isMultiDataSource");
                if (dbMultiDataSource == true) {
                    listBeanName.add(UcMultiDataSource.class.getName());
                } else {
                    listBeanName.add(UcSingleDataSource.class.getName());
                }

                arrayBeanName = listBeanName.toArray(new String[listBeanName.size()]);
            }
        }
        return arrayBeanName;
    }


    public static String getDbConfigFilePathName() {
        return dbConfigFilePathName;
    }

    public static String getDbMapperPath() {
        return dbMapperPath;
    }

    public static boolean isDbMultiDataSource() {
        return dbMultiDataSource;
    }

    public static boolean isDbShowWeb() {
        return dbShowWeb;
    }
}
