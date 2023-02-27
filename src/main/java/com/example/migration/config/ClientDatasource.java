package com.example.migration.config;

import lombok.NoArgsConstructor;

import javax.sql.DataSource;

@NoArgsConstructor
public class ClientDatasource {
    public static DataSource dataSource;
    public ClientDatasource(DataSource dataSource){
        System.out.println("ClientDatasource 생성자");
        this.dataSource = dataSource;
    }

    public static DataSource getDatasource(ClientDatabase clientDatabase) {
        ClientDatabaseContextHolder.set(clientDatabase);
        return dataSource;
    }
}
