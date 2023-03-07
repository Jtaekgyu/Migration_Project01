package com.example.migration.service;

import com.example.migration.config.ClientDatabase;
import com.example.migration.config.ClientDatasource;
import com.example.migration.controller.dto.request.MigrationReqDto;
import com.example.migration.controller.dto.request.OracleInfoReqDto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.*;
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
        Statement stmt = null;
        Connection oraConn = null;
        ResultSet rs = null;
        try {
            oraConn = ORAdbConnect(oraConn, oraReqDto.getHost(), oraReqDto.getPort(),
                    oraReqDto.getSid(), oraReqDto.getUsername(), oraReqDto.getPassword());
            if(stmt ==null)
                stmt = oraConn.createStatement();
//            rs = stmt.executeQuery("SELECT * FROM ALL_OBJECTS WHERE OWNER = 'TEST1'");
            rs = stmt.executeQuery("SELECT table_name FROM all_tables WHERE owner = 'TEST1'");
//            List<OraTableListResDto> oraTableListResDtos = new ArrayList<>();
            List<String> oraTableListResDtos = new ArrayList<>();
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

    public Object testMigration(MigrationReqDto mgReqDto) {
        Statement stmt = null;
        PreparedStatement pstmt = null;
        PreparedStatement pstmt2 = null;
        Connection oraConn = null;
        Connection posConn = null;
        ResultSet rs = null;
        ResultSet rs2 = null;
        String query = null;
        List<String> tableList = new ArrayList<>();

        for(String table: mgReqDto.getOracleTableList()){
            tableList.add(table);
        }

        // 테이블 목록을 조회한다.(그래도 table이 스키마에 있는지 확인해야하니까 조건문 사용한다.)
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT table_name FROM all_tables WHERE owner = ? ");
// mgReqDto.getOraReqDto().getUsername()
        sb.append("AND TABLE_NAME IN (");
        for (int i = 0; i < tableList.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("'" + tableList.get(i) + "'");
        }
        sb.append(")");
        query = sb.toString();

        try {
            oraConn = ORAdbConnect(oraConn, mgReqDto.getOraReqDto().getHost(), mgReqDto.getOraReqDto().getPort(),
                    mgReqDto.getOraReqDto().getSid(), mgReqDto.getOraReqDto().getUsername(), mgReqDto.getOraReqDto().getPassword());
            posConn = POSdbConnect();

            if(pstmt == null)
                pstmt = oraConn.prepareStatement(query); // prepareStatement를 호출하여 pstmt 변수에 쿼리를 실행할 객체를 할당한다.
            pstmt.setString(1, mgReqDto.getOraReqDto().getUsername());
//            for(int i = 0; i < tableList.size(); i++){
//                pstmt.setString(i + 2, tableList.get(i));
//            }
            rs = pstmt.executeQuery(); // executeQuery로 쿼리를 실행하고 그 값을 rs에 할당한다. () 여기안에 쿼리의 final String 값넣어도 될듯(물론 그걸로 처음부터 과정을 해야함)
            while (rs.next()){
                String table = rs.getString(1); // 여기서 table 목록 조회했으니까 이거가지고 select 하자
                System.out.println("table : " + table);

                sb.setLength(0); // sb를 초기화 하는 가장 빠른 방법
                // 해당 쿼리 조회하면 table별 column_name, constraint_type, search_condition을 사용하면 되는데
                // 제약조건이 여러 개 있으면 column_name은 중복 되므로 column_name들을 set에 담자 그리고 while 돌떄 마다 set을 초기화 해주자.
//                sb.append("SELECT TABLE_NAME, COLUMN_NAME FROM all_tab_cols WHERE OWNER ='"+mgReqDto.getOraReqDto().getUsername()+"' AND TABLE_NAME = '"+table+"'");
                sb.append("SELECT tabcols.column_id, tabcols.column_name, tabcols.data_type || '(' || tabcols.data_length || ')' as data_type_length, cons.constraint_type, cons.constraint_name, cons.search_condition\n" +
                        "FROM all_tab_cols tabcols\n" +
                        "LEFT JOIN all_cons_columns cols\n" +
                        "  ON tabcols.owner = cols.owner\n" +
                        " AND tabcols.table_name = cols.table_name\n" +
                        " AND tabcols.column_name = cols.column_name\n" +
                        "LEFT JOIN all_constraints cons\n" +
                        "  ON cols.owner = cons.owner\n" +
                        " AND cols.constraint_name = cons.constraint_name\n" +
                        "WHERE tabcols.owner = '"+mgReqDto.getOraReqDto().getUsername()+"'\n" +
                        " AND tabcols.table_name = '"+table+"'\n" +
                        "ORDER BY tabcols.column_id"); // 뒤에 ; 찍으면 ORA-00911: invalid character 에러 발생한다..
                query = sb.toString();
                if(pstmt2 == null) // 여기서 안들어가서 그러네
                    pstmt2 = oraConn.prepareStatement(query);

                // 위 에서 조회한 column_name, constraint_type, search_conditiond을 사용해서 create 문을 만든다.
                // 테이블을 create할 때는
                StringBuilder createSb = new StringBuilder();
                createSb.append("CREATE TABLE " +table+" (\n");
                rs2 = pstmt2.executeQuery();

                Set<String> columNameSet = new HashSet<>();
                String tmpColumnName;
                String dataType;
                String constraintType;
                String searchCondition;
                String enter = null;
                StringBuilder constraintSb = new StringBuilder();
                boolean chk = true;
                int idx = 1;
                while (rs2.next()){
                    tmpColumnName = rs2.getString("COLUMN_NAME"); // 값이 덮어 씌워지는 이유는 pstmt 객체를 초기화 하지않아서 그랬다.
                    dataType = rs2.getString("DATA_TYPE_LENGTH");
                    constraintType = rs2.getString("CONSTRAINT_TYPE");
                    searchCondition = rs2.getString("SEARCH_CONDITION");

                    if( !columNameSet.contains(tmpColumnName) && idx >= 2){ // set에 없거나 2반쩨 칼럼부터
                        enter = ",\n";
                    } else {
                        enter = "";
                    }

                    if(enter !=null && enter.equals(",\n")){
                        createSb.append(enter);
                    }

                    if( columNameSet.add(tmpColumnName) ){ // 존재하지 않으면 true, 존재하면 false를 반환
                        createSb.append(tmpColumnName);
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
                System.out.println("~~~createSb : \n" + createSb);
                columNameSet.clear();
                rs2.close();
                pstmt2 = null; // ★★★ 이거 해주니까 되네.....
//                ResultSetMetaData rsmd = rs2.getMetaData();
//                int columnCount = rsmd.getColumnCount();
                /*for(int i = 1; i <= columnCount; i++){
                    String columnName = rsmd.getColumnName(i);
                    String columnTypeName = rsmd.getColumnTypeName(i); // 이걸로 데이터 타입 뽑자
                    int columLeng = rsmd.getColumnDisplaySize(i); // 컬럼의 길이(데이터의 길이 아님)
                    int isNull = rsmd.isNullable(i); // 0: Null허용안함, 1 : Null허용, 2: 알 수 없음
                    System.out.println("~~Column name: " + columnName +", Type : " +columnTypeName+ ", columLeng : " + columLeng + ", isNull : " + isNull);
                    // 여기서 oracle 타입을 postgresql로 변경해야한다.

//                    switch (columnName) {
//                        case "NUMBER" :
//                    }
//                    createSb.append(columnName+)
                }*/
                // 이 meta데이터 추추한걸
                // postgresql로 바로 create할건지, 아니면 제약조건까지 조회한다음에 create할건지
                // 물론 순서는 테이블 create, 데이터 insert, 테이블에 제약조건 추가

            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
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
