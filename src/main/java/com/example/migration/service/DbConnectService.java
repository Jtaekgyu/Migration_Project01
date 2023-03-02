package com.example.migration.service;

import com.example.migration.config.ClientDatabase;
import com.example.migration.config.ClientDatasource;
import com.example.migration.controller.dto.request.OracleInfoReqDto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

//import tremendous.ora2pg.jdbc.Ora2Pg;
//import org.ora2pg.Ora2Pg;

@Service
@RequiredArgsConstructor
public class DbConnectService {

    public Object getData(DataSource dataSource) throws SQLException {
        System.out.println("~~~~~~getData");
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT *FROM TBL_TEAM;");
            List<HashMap<Object, String>> resultList = new ArrayList<>();

            while (rs.next()){
                HashMap<Object, String> obj = new HashMap<>();
                obj.put("id", rs.getString(1));
                obj.put("name", rs.getString(2));
                obj.put("region", rs.getString(3));
                obj.put("description", rs.getString(4));
                resultList.add(obj);
            }
            return resultList;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            rs.close();
            stmt.close();
            conn.close();
        }
    }

    public Object testMigration1(OracleInfoReqDto oracleInfoReqDto) throws SQLException {
        // Oracle DB 연결
        String oracleUrl = "jdbc:oracle:thin:@"+oracleInfoReqDto.getHost()+":1521:xe";
        String oracleUser = oracleInfoReqDto.getUsername();
        String oraclePassword = oracleInfoReqDto.getPassword();
        Connection oracleConn = DriverManager.getConnection(oracleUrl, oracleUser, oraclePassword);

        // PostgreSQL DB 연결
        String postgreUrl = "jdbc:postgresql://localhost:6434/agens_migration";
        String postgreUser = "agens";
        String postgrePassword = "1234";
        Connection postgreConn = DriverManager.getConnection(postgreUrl, postgreUser, postgrePassword);

        // 데이터 추출
        Statement stmt = oracleConn.createStatement();
//        ResultSet rs = stmt.executeQuery("SELECT * FROM tbl_user");
        ResultSet rs = stmt.executeQuery("SELECT *FROM TBL_TEAM;");

        // 데이터 적재
        PreparedStatement pstmt = postgreConn.prepareStatement("INSERT INTO TBL_TEAM (name, region, description) VALUES (?, ?, ?);");
        //PreparedStatement pstmt = postgreConn.prepareStatement("INSERT INTO tbl_user VALUES (?, ?, ?, ?, ?)");
        while (rs.next()) {
//            int id = rs.getInt("id");
            String name = rs.getString("name");
            String region = rs.getString("region");
            String description = rs.getString("description");

//            pstmt.setInt(1, id);
            pstmt.setString(1, name);
            pstmt.setString(2, region);
            pstmt.setString(3, description);
            pstmt.executeUpdate();
            /*int id = rs.getInt("id");
            String name = rs.getString("name");
            int age = rs.getInt("age");
            String address = rs.getString("address");
            int teamId = rs.getInt("team_id");

            pstmt.setInt(1, id);
            pstmt.setString(2, name);
            pstmt.setInt(3, age);
            pstmt.setString(4, address);
            pstmt.setInt(5, teamId);
            pstmt.executeUpdate();*/
        }

        // DB 연결 해제
        rs.close();
        stmt.close();
        pstmt.close();
        oracleConn.close();
        postgreConn.close();

        return null;
    }

   public Object testMigration2(OracleInfoReqDto oracleInfoReqDto) {
        Object result = null;
        // Oracle DB 연결 준비
        String oracleUrl = "jdbc:oracle:thin:@"+oracleInfoReqDto.getHost()+":1521:xe";
        String oracleUser = oracleInfoReqDto.getUsername();
        String oraclePassword = oracleInfoReqDto.getPassword();

        // PostgreSQL DB 연결 준비
        String postgreUrl = "jdbc:postgresql://localhost:6434/agens_migration";
        String postgreUser = "agens";
        String postgrePassword = "1234";

        try { // 1. Oracle 객체 정보 추출
            Connection oracleConn = DriverManager.getConnection(oracleUrl, oracleUser, oraclePassword);
            Statement stmt = oracleConn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM ALL_OBJECTS WHERE OWNER = 'TEST1'");

            Ora2pg ora2pg = new Ora2pg();
            ora2pg.setOraHome("/u01/app/oracle/product/11.2.0/xe");
            ora2pg.setPgHome("/usr/local");
            ora2pg.setUser("agens");
            ora2pg.setPassword("1234");
            ora2pg.setDsn("DBI:Pg:database=agens_migration;host=localhost;port=6434");
            ora2pg.setVerbose(true); // migration 과정에서 자세한 로그 메시지
            ora2pg.setDebug(true); // migration 과정에서 디버그 메시지
            ora2pg.setConfig("src/main/resources/config/ora2pg.conf"); // ora2pg 실행에 필요한 설정 파일 경로 설정
            ora2pg.setExportDir("src/main/resources/tmp/ora2pg"); // 마이그레이션 결과를 저장할 디렉토리

            while (rs.next()){
                String objectType = rs.getString("OBJECT_TYPE");
                String objectName = rs.getString("OBJECT_NAME");
//                fileSave(stmt, objectType, objectName);
//                String ddl = generateDDL(stmt, objectType, objectName);
                String ddl = "SELECT DBMS_METADATA.GET_DDL('" + objectType + "','" + objectName + "') FROM DUAL";
                ora2pg.setInfile(ddl); // 마이그레이션 대상 DDL 파일의 경로를 설정합니다.
                ora2pg.exportSchema(); // 메소드는 마이그레이션된 결과 파일의 경로를 반환합니다. 이 결과 파일은 .sql 확장자를 갖는다.

                executeDDL(ddl, ora2pg, postgreUrl, postgreUser, postgrePassword);
                ora2pg.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private void executeDDL(String ddl, Ora2pg ora2pg, String postgreUrl, String postgreUser, String postgrePassword) {
        try {
            Connection postgreConn = DriverManager.getConnection(postgreUrl, postgreUser, postgrePassword);
            Statement stmt = postgreConn.createStatement();
            stmt.execute(ora2pg.getInfile() + ".sql");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /*public String generateDDL(Statement stmt, String objectType, String objectName) {
        try {
            ResultSet rs = stmt.executeQuery("SELECT DBMS_METADATA.GET_DDL('" + objectType + "','" + objectName + "') FROM DUAL");
            if(rs.next()){
                String ddl = rs.getString(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }*/


    /*private String generateDDL(Statement stmt, String objectType, String objectName) {
        try { // rs에는 각 objectType과 objectName 조건에 맞는 DDL(CREATE쿼리)이 생성된다.
            ResultSet rs = stmt.executeQuery("SELECT DBMS_METADATA.GET_DDL('" + objectType + "','" + objectName + "') FROM DUAL");
            *//*if(rs.next()){
                Object result = rs.getString(1); // 확인용
                return rs.getString(1);
            }*//*
            if(rs.next()){
                String ddl = rs.getString(1);
                try {
                    File file = new File("src/main/resources/sql/input_file.sql");
                    FileWriter fw = new FileWriter(file, true); // true 파라미터는 append 모드로 설정합니다.
                    fw.write(ddl);
                    fw.close();

                    // ora2pg 명령어를 실행하기 위한 ProcessBuilder 객체 생성
                    ProcessBuilder pb = new ProcessBuilder("src/main/resources/config/ora2pg", "-c", "src/main/resources/config/config_file.conf", "-i", "src/main/resources/sql/input_file.sql", "-o", "src/main/resources/sql/output_file.sql");

                    // ora2pg 명령어 실행
                    Process p = pb.start();

                    // ora2pg 명령어 실행이 완료될 때까지 대기
                    p.waitFor();

                    // PostgreSQL DDL 읽기
                    BufferedReader br = new BufferedReader(new FileReader("src/main/resources/sql/output_file.sql"));

                    StringBuilder sb = new StringBuilder();
                    String line;
                    while ((line = br.readLine()) != null) {
                        sb.append(line).append("\n");
                    }
                    br.close();

                    return sb.toString();
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }*/

    /*public void fileSave(Statement stmt, String objectType, String objectName){
        try {
            ResultSet rs = stmt.executeQuery("SELECT DBMS_METADATA.GET_DDL('" + objectType + "','" + objectName + "') FROM DUAL");

            if(rs.next()){
                String ddl = rs.getString(1);
                FileWriter fw = null;
                try {
                    fw = new FileWriter("input_file.sql");
                    fw.write(ddl);
                    fw.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }*/

    /*public Object testMigration3byOra2pg(OracleInfoReqDto oracleInfoReqDto) throws SQLException {
        DataSource dataSource = null;
        dataSource = ClientDatasource.getDatasource(ClientDatabase.ORACLE);

        // ora2pg 연결 정보 설정
        String ora2pgUrl = "jdbc:postgresql://localhost:6434/agens_migration";
        Properties ora2pgProps = new Properties();
        ora2pgProps.setProperty("user", "agens");
        ora2pgProps.setProperty("password", "1234");
        ora2pgProps.setProperty("ssl", "false");

        Connection ora2pgConn = null;
        Connection dataSourceConn = null;
        try {
            // ora2pg 연결 생성
            ora2pgConn = DriverManager.getConnection(ora2pgUrl, ora2pgProps);

            // 데이터 소스 연결 생성
            dataSourceConn = dataSource.getConnection(); // 지금은 오라클

            // 데이터 이전을 위한 ora2pg 세션 시작
            Statement ora2pgStmt = ora2pgConn.createStatement();
            ora2pgStmt.execute("BEGIN");

            // 데이터 이전 쿼리 작성 및 실행
            String sql = "SELECT * FROM TBL_TEST1";
            Statement dataSourceStmt = dataSourceConn.createStatement();
            ResultSet rs = dataSourceStmt.executeQuery(sql);
            while (rs.next()) {
                // 데이터를 ora2pg로 전송
                // 이전 쿼리에 대한 예시입니다.
                ora2pgStmt.execute(String.format(
                        "INSERT INTO TBL_PG_TEST1 (id, name, content) VALUES (%d, '%s', %s)",
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getString("content")
                ));
            }

            // ora2pg 세션 종료
            ora2pgStmt.execute("COMMIT");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            // 연결 종료
            ora2pgConn.close();
            dataSourceConn.close();
        }
        return null; // 일단 null로 리턴
    }*/
}
