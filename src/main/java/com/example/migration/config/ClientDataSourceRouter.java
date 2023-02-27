package com.example.migration.config;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

public class ClientDataSourceRouter extends AbstractRoutingDataSource {

    @Override
    protected Object determineCurrentLookupKey() {
        System.out.println("ClientDataSourceRouter determineCurrentLookupKey 호출");
        return ClientDatabaseContextHolder.getClientDatabase();
    }
}
