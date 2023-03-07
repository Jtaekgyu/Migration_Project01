package com.example.migration.controller;

import com.example.migration.config.ClientDatabase;
import com.example.migration.config.ClientDatasource;
import com.example.migration.controller.dto.request.MigrationReqDto;
import com.example.migration.controller.dto.request.OracleInfoReqDto;
import com.example.migration.service.DbConnectService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.sql.DataSource;
import java.sql.SQLException;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api")
public class DbConnectController {

    private final DbConnectService dbConnectService;

    @GetMapping("/datasource/{dbName}")
    public ResponseEntity<?> getData(@PathVariable String dbName) throws SQLException {
        Object result = null;
        DataSource dataSource = null;
        if("agens".equals(dbName)){
            dataSource = ClientDatasource.getDatasource(ClientDatabase.AGENS);
            result = dbConnectService.getData(dataSource);
        }
        else if("oracle".equals(dbName)){
            dataSource = ClientDatasource.getDatasource(ClientDatabase.ORACLE);
            result = dbConnectService.getData(dataSource);
        }
        return ResponseEntity.ok(result);
    }

    @PostMapping
    public ResponseEntity<?> testMigration(@RequestBody OracleInfoReqDto oracleInfoReqDto) throws SQLException {
        Object result = dbConnectService.testMigration2(oracleInfoReqDto);
        return ResponseEntity.ok(result);
    }

    @GetMapping("/oracle")
    public ResponseEntity<?> getTableList(@RequestBody OracleInfoReqDto oracleInfoReqDto){
        Object result = dbConnectService.getTableList(oracleInfoReqDto);
        return ResponseEntity.ok(result);
    }

    @PostMapping("/migration")
    public ResponseEntity<?> migration(@RequestBody MigrationReqDto migrationReqDto){
        Object result = dbConnectService.testMigration(migrationReqDto);
        return ResponseEntity.ok(result);
    }

}
