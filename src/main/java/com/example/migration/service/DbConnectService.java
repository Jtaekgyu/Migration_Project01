package com.example.migration.service;

import com.example.migration.controller.dto.request.MigrationReqDto;
import com.example.migration.controller.dto.request.OracleInfoReqDto;
import com.example.migration.queryHelper.OracleQueryHelper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

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

    public Connection ORAdbConnect(Connection oracleConn, String host, String port, String sid, String username, String password) throws SQLException {
        String connectionUrlFormat = "jdbc:oracle:thin:@%s:%s:%s";
        String connecttionUrl = String.format(connectionUrlFormat, host, port, sid);
        oracleConn = DriverManager.getConnection(connecttionUrl, username, password);
        System.out.println("oracle db connectionUrl : " + connecttionUrl);
        return oracleConn;
    }

    public Connection POSdbConnect() throws SQLException {
        // PostgreSQL DB 연결
        String postgreUrl = "jdbc:postgresql://localhost:6434/agens_migration";
        String postgreUser = "agens";
        String postgrePassword = "1234";
        Connection postgreConn = DriverManager.getConnection(postgreUrl, postgreUser, postgrePassword);
        return postgreConn;
    }

    public Object getTableList(OracleInfoReqDto oraReqDto){
        OracleQueryHelper queryHelper = new OracleQueryHelper();
        PreparedStatement pstmt = null;
        Connection oraConn = null;
        ResultSet rs = null;
        List<String> oraTableListResDtos = new ArrayList<>();

        try {
            oraConn = ORAdbConnect(oraConn, oraReqDto.getHost(), oraReqDto.getPort(),
                    oraReqDto.getSid(), oraReqDto.getUsername(), oraReqDto.getPassword());
            if(pstmt == null)
                pstmt = oraConn.prepareStatement(OracleQueryHelper.SELECT_ALL_TABLES);
            pstmt.setString(1, oraReqDto.getUsername());

            rs = pstmt.executeQuery();
            while (rs.next()){
//                String objectType = rs.getString("OBJECT_TYPE");
                String objectName = rs.getString("TABLE_NAME");
                oraTableListResDtos.add(objectName);
                /*oraTableListResDtos.add(OraTableListResDto.builder()
                        .table(objectName)
                        .build()
                );*/
            }
            return oraTableListResDtos;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Object oracleToPostgresMigration(MigrationReqDto mgReqDto) throws SQLException{
        Statement posgresStmt = null;
        PreparedStatement oraPstmt = null;
        Connection oraConn = null;
        Connection posConn = null;
        ResultSet rs = null;
        String query = null;
        List<String> tableList = new ArrayList<>();

        for(String table: mgReqDto.getOracleTableList()){
            tableList.add(table);
        }

        // 테이블 목록을 조회한다.(그래도 table이 스키마에 있는지 확인해야하니까 조건문 사용한다.)
        StringBuilder sb = new StringBuilder(); // 지금은 sb한 의미가 사라짐
        StringBuilder createSb = new StringBuilder();
        try {
            oraConn = ORAdbConnect(oraConn, mgReqDto.getOraReqDto().getHost(), mgReqDto.getOraReqDto().getPort(),
                    mgReqDto.getOraReqDto().getSid(), mgReqDto.getOraReqDto().getUsername(), mgReqDto.getOraReqDto().getPassword());
            posConn = POSdbConnect();

            for(String table : tableList){
                sb.setLength(0); // sb를 초기화 하는 가장 빠른 방법
                // 제약조건이 여러 개 있으면 column_name은 중복 되므로 column_name들을 set에 담자 그리고 while 돌떄 마다 set을 초기화 해주자.
                sb.append(OracleQueryHelper.SELECT_TABLE_INFO); // 뒤에 ; 찍으면 ORA-00911: invalid character 에러 발생한다..
                query = sb.toString();
                if(oraPstmt == null) // 여기서 안들어가서 그러네
                    oraPstmt = oraConn.prepareStatement(query);

                oraPstmt.setString(1, mgReqDto.getOraReqDto().getUsername());
                oraPstmt.setString(2, table);
                // 위 에서 조회한 column_name, constraint_type, search_conditiond을 사용해서 create 문을 만든다.
                // 테이블을 create할 때는
                createSb.append("CREATE TABLE " +table+" (\n");
                rs = oraPstmt.executeQuery();

                Set<String> columNameSet = new HashSet<>();
                String tmpColumnName;
                String dataType;
                String dataLength;
                String constraintType;
                String searchCondition;
                String enter = null;
                StringBuilder constraintSb = new StringBuilder();
                boolean chk = true;
                int idx = 1;
                while (rs.next()){
                    tmpColumnName = rs.getString("COLUMN_NAME"); // 값이 덮어 씌워지는 이유는 pstmt 객체를 초기화 하지않아서 그랬다.
                    dataType = rs.getString("DATA_TYPE");
                    dataLength = rs.getString("DATA_LENGTH");
                    constraintType = rs.getString("CONSTRAINT_TYPE");
                    searchCondition = rs.getString("SEARCH_CONDITION");

                    // 이렇게 Set에 ColumnName이 없고 두 번째 컬럼부터 ,엔터를 입력하면 쿼리 형식이 맞는다
                    // 현재 쿼리가 컬럼을 제약조건 마다 조회하기 때문에 한 컬럼에 제약조건이 여러개 있으면 일일이 행으로 조회된다.
                    // 그러므로 이전에 set에 행이 있으면 여러번 조회되는 컬럼이기 때문에 ",\n" 를입력하지 않는다.
                    if( !columNameSet.contains(tmpColumnName) && idx >= 2){
                        enter = ",\n";
                    } else {
                        enter = "";
                    }

                    if(enter !=null && enter.equals(",\n")){
                        createSb.append(enter);
                    }
                    if( columNameSet.add(tmpColumnName) ){ // 존재하지 않으면 true, 존재하면 false를 반환
                        createSb.append(tmpColumnName);
                        String realDataType;
                        switch (dataType) {
                            case "NUMBER" : dataType = "INTEGER";
                                break;
                            case "CHAR" : dataType = "CHAR";
                                break;
                            case  "NCHAR" : dataType = "CHAR";
                                break;
                            case "VARCHAR" : dataType = "VARCHAR";
                                break;
                            case "VARCHAR2" : dataType = "VARCHAR";
                                break;
                            case  "NVARCHAR2" :dataType = "VARCHAR";
                                break;
                        }
                        if(!dataType.equals("INTEGER"))
                            createSb.append(" "+dataType+"("+dataLength+")");
                        else // 오라클에서 타입이 INTEGER 이면 (Length)안붙임
                            createSb.append(" "+dataType);
                    }
                    // 제약조건이 없으면 다음 컬럼을 탐색한다.
                    if(constraintType == null){
                        continue;
                    }
                    if( constraintType.equals("P") )
                        constraintSb.append(" PRIMARY KEY");

                    if( constraintType.equals("C") ){
                        if( searchCondition.contains("NOT NULL") ) // 바깥 조건문이 "C"일 때만 들어와서 NullPointerException은 없을거다.
                            constraintSb.append(" NOT NULL");
                        else
                            constraintSb.append(" CHECK ("+searchCondition+")");
                    }
                    if( constraintType.equals("U"))
                        constraintSb.append(" UNIQUE");

                    createSb.append(constraintSb);
                    constraintSb.setLength(0);
                    idx++;
                }
                createSb.append("\n);");
                System.out.println("\n" + createSb);
                columNameSet.clear();
                rs.close();
                oraPstmt = null; // ★★★ 이거 해주니까 되네.....
//                ResultSetMetaData rsmd = rs2.getMetaData();
//                int columnCount = rsmd.getColumnCount();
                if(posgresStmt == null){
                    posgresStmt = posConn.createStatement();
//                    System.out.println("~~~ posgresStmt == null");
                }
                posgresStmt.execute(String.valueOf(createSb));
                createSb.setLength(0);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
//            rs.close();
//            oraPstmt.close();
            oraConn.close();
//            posgresStmt.close();
        }
        return null;
    }

//    public Object

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

            while (rs.next()){
                String objectType = rs.getString("OBJECT_TYPE");
                String objectName = rs.getString("OBJECT_NAME");
//                fileSave(stmt, objectType, objectName);
//                String ddl = generateDDL(stmt, objectType, objectName);
                String ddl = "SELECT DBMS_METADATA.GET_DDL('" + objectType + "','" + objectName + "') FROM DUAL";

                executeDDL(ddl, postgreUrl, postgreUser, postgrePassword);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private void executeDDL(String ddl, String postgreUrl, String postgreUser, String postgrePassword) {
        try {
            Connection postgreConn = DriverManager.getConnection(postgreUrl, postgreUser, postgrePassword);
            Statement stmt = postgreConn.createStatement();
            stmt.execute(ddl);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
