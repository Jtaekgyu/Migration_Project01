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
        // PostgreSQL DB ??????
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

        // ????????? ????????? ????????????.(????????? table??? ???????????? ????????? ????????????????????? ????????? ????????????.)
        StringBuilder sb = new StringBuilder(); // ????????? sb??? ????????? ?????????
        StringBuilder createSb = new StringBuilder();
        try {
            oraConn = ORAdbConnect(oraConn, mgReqDto.getOraReqDto().getHost(), mgReqDto.getOraReqDto().getPort(),
                    mgReqDto.getOraReqDto().getSid(), mgReqDto.getOraReqDto().getUsername(), mgReqDto.getOraReqDto().getPassword());
            posConn = POSdbConnect();

            for(String table : tableList){
                sb.setLength(0); // sb??? ????????? ?????? ?????? ?????? ??????
                // ??????????????? ?????? ??? ????????? column_name??? ?????? ????????? column_name?????? set??? ?????? ????????? while ?????? ?????? set??? ????????? ?????????.
                sb.append(OracleQueryHelper.SELECT_TABLE_INFO); // ?????? ; ????????? ORA-00911: invalid character ?????? ????????????..
                query = sb.toString();
                if(oraPstmt == null) // ????????? ??????????????? ?????????
                    oraPstmt = oraConn.prepareStatement(query);

                oraPstmt.setString(1, mgReqDto.getOraReqDto().getUsername());
                oraPstmt.setString(2, table);
                // ??? ?????? ????????? column_name, constraint_type, search_conditiond??? ???????????? create ?????? ?????????.
                // ???????????? create??? ??????
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
                    tmpColumnName = rs.getString("COLUMN_NAME"); // ?????? ?????? ???????????? ????????? pstmt ????????? ????????? ??????????????? ?????????.
                    dataType = rs.getString("DATA_TYPE");
                    dataLength = rs.getString("DATA_LENGTH");
                    constraintType = rs.getString("CONSTRAINT_TYPE");
                    searchCondition = rs.getString("SEARCH_CONDITION");

                    // ????????? Set??? ColumnName??? ?????? ??? ?????? ???????????? ,????????? ???????????? ?????? ????????? ?????????
                    // ?????? ????????? ????????? ???????????? ?????? ???????????? ????????? ??? ????????? ??????????????? ????????? ????????? ????????? ????????? ????????????.
                    // ???????????? ????????? set??? ?????? ????????? ????????? ???????????? ???????????? ????????? ",\n" ??????????????? ?????????.
                    if( !columNameSet.contains(tmpColumnName) && idx >= 2){
                        enter = ",\n";
                    } else {
                        enter = "";
                    }

                    if(enter !=null && enter.equals(",\n")){
                        createSb.append(enter);
                    }
                    if( columNameSet.add(tmpColumnName) ){ // ???????????? ????????? true, ???????????? false??? ??????
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
                        else // ??????????????? ????????? INTEGER ?????? (Length)?????????
                            createSb.append(" "+dataType);
                    }
                    // ??????????????? ????????? ?????? ????????? ????????????.
                    if(constraintType == null){
                        continue;
                    }
                    if( constraintType.equals("P") )
                        constraintSb.append(" PRIMARY KEY");

                    if( constraintType.equals("C") ){
                        if( searchCondition.contains("NOT NULL") ) // ?????? ???????????? "C"??? ?????? ???????????? NullPointerException??? ????????????.
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
                oraPstmt = null; // ????????? ?????? ???????????? ??????.....
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
        // Oracle DB ?????? ??????
        String oracleUrl = "jdbc:oracle:thin:@"+oracleInfoReqDto.getHost()+":1521:xe";
        String oracleUser = oracleInfoReqDto.getUsername();
        String oraclePassword = oracleInfoReqDto.getPassword();

        // PostgreSQL DB ?????? ??????
        String postgreUrl = "jdbc:postgresql://localhost:6434/agens_migration";
        String postgreUser = "agens";
        String postgrePassword = "1234";

        try { // 1. Oracle ?????? ?????? ??????
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
