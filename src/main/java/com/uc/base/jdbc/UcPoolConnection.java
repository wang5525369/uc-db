package com.uc.base.jdbc;

import com.google.common.collect.Lists;
import com.uc.base.oper.DbOper;
import org.springframework.util.CollectionUtils;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class UcPoolConnection {
    private List<UcConnection> listUcConnection = Lists.newArrayList();
    private Properties properties;
    private DbOper dbOper;

    public synchronized void init(Properties properties, DbOper dbOper) throws UnknownHostException, SQLException, URISyntaxException {
        this.properties = properties;
        this.dbOper = dbOper;
        UcConnection ucEsConnection = new UcConnection(properties,dbOper);
        listUcConnection.add(ucEsConnection);
    }

    public synchronized UcConnection getConnection() throws UnknownHostException, SQLException, URISyntaxException {
        UcConnection ucConnection = null;
        if (CollectionUtils.isEmpty(listUcConnection) == false){
            ucConnection = listUcConnection.get(0);
        }else{
            ucConnection = new UcConnection(properties,dbOper);
        }
        return ucConnection;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public synchronized void closeAllConnection(){
        for(UcConnection ucConnection : listUcConnection){
            ucConnection.realClose();
        }
    }
}
