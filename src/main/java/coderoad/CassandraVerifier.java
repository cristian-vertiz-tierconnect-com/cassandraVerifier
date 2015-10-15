package coderoad;

import com.datastax.driver.core.*;
import com.datastax.driver.core.PreparedStatement;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.List;

public class CassandraVerifier {

    private static String[] car = {"|", "/" , "-", "\\"};
    private static String database;

    public static void verify() throws Exception {
        StringBuilder sb = new StringBuilder();

        Map<Long, List<Long>> thingTimeCount = new HashMap<Long, List<Long>>();
        int countFVH2 = 0, countFVH=0, countFV=0, countFT=0, fvh2Orphan = 0, fvOrphan = 0;

        CassandraUtils.init("127.0.0.1", "riot_main");
        System.out.println("\n\n\n");

        Map<Long, Long> thingFieldMap = getThingFieldMap();

        //field_value_history2
        PreparedStatement selectFVH2 = CassandraUtils.getSession().prepare("SELECT thing_id, time FROM field_value_history2 limit 1000000000");
        for (Row row : CassandraUtils.getSession().execute(new BoundStatement(selectFVH2)) ){

            Long thing_id = row.getLong("thing_id");
            Long time = row.getDate("time").getTime();

            List<Long> temp = new ArrayList<Long>();
            if(thingTimeCount.containsKey(thing_id)){
                temp = thingTimeCount.get(thing_id);
            }
            if(!temp.contains(time))
                temp.add(time);

            thingTimeCount.put(thing_id,temp);
            countFVH2++;

            if(countFVH2 % 100000 == 0){
                System.out.print("\rAnalysing cassandra field_value_history2 " + car[(countFVH2/100000)%4]);
            }

        }

        System.out.println("Analysing cassandra field_value_history2  [OK]");


        //field_value_history
        PreparedStatement selectFVH = CassandraUtils.getSession().prepare("SELECT field_id FROM field_value_history limit 1000000000");
        for (Row row : CassandraUtils.getSession().execute(new BoundStatement(selectFVH)) ){
            if(!thingFieldMap.containsKey(row.getLong("field_id")))
                fvh2Orphan += 1;
            countFVH++;
            if(countFVH % 100000 == 0){
                System.out.print("\rAnalysing cassandra field_value_history " + car[(countFVH/100000)%4]);
            }
        }

        System.out.println("\rAnalysing cassandra field_value_history [OK]");


        //field_type
        PreparedStatement selectFT = CassandraUtils.getSession().prepare("SELECT count(*) as count FROM field_type limit 1000000000");
        Row rowFT = CassandraUtils.getSession().execute(new BoundStatement(selectFT)).all().get(0);
        countFT = Integer.parseInt(rowFT.getLong("count")+"");
        System.out.println("\rAnalysing cassandra field_value_history [OK]");



        //field_value
        PreparedStatement selectFV = CassandraUtils.getSession().prepare("SELECT field_id FROM field_value limit 1000000000");
        for (Row row : CassandraUtils.getSession().execute(new BoundStatement(selectFV)) ){
            if(!thingFieldMap.containsKey(row.getLong("field_id")))
                fvOrphan += 1;
            countFV++;
            if(countFV % 100000 == 0){
                System.out.print("\rAnalysing cassandra field_value " + car[(countFV/100000)%4]);
            }
        }

        System.out.println("\rAnalysing cassandra field_value [OK]");




//        System.out.println("thing_id => blinks_count");
        sb.append("thing_id => blinks_count");

//        System.out.println("Cassandra field_value_history2 results :");
        sb.append("\nCassandra field_value_history2 results :");

        Long totalBLinks = 0L;
        for(Map.Entry<Long,List<Long>> entry : thingTimeCount.entrySet()){
            totalBLinks += entry.getValue().size();
//            System.out.println(entry.getKey() + " => " + entry.getValue().size());
            sb.append("\n" + entry.getKey() + " => " + entry.getValue().size());
        }

        System.out.println("================================ Summary ================================");
        System.out.println("Total blinks --------------------------------------> " + totalBLinks);
        System.out.println("Things --------------------------------------------> " + thingTimeCount.entrySet().size());
        System.out.println("(1)Total cassandra rows in field_value_history2 ------> " + countFVH2);
        System.out.println("(2)Total cassandra orphans in field_value_history2 ---> " + fvh2Orphan);
        System.out.println("(3)Total cassandra rows in field_value_history -------> " + countFVH);
        System.out.print("\n");
        System.out.println("(4)Total cassandra rows in field_type ----------------> " + countFT);
        System.out.println("(5)Total cassandra orphans in field_value ------------> " + fvOrphan);
        System.out.println("(6)Total cassandra rows in field_value ---------------> " + countFV);
        System.out.println("\n***NOTE : Make sure (1)+(2)=(3) and (4)+(5)=(6)");

        sb.append("\n================================ Summary ================================");
        sb.append("\nTotal blinks --------------------------------------> " + totalBLinks);
        sb.append("\nThings --------------------------------------------> " + thingTimeCount.entrySet().size());
        sb.append("\nTotal cassandra rows in field_value_history2 ------> " + countFVH2);
        sb.append("\nTotal cassandra orphans in field_value_history2 ---> " + fvh2Orphan);
        sb.append("\nTotal cassandra rows in field_value_history -------> " + countFVH);
        sb.append("\n");
        sb.append("Total cassandra rows in field_type ----------------> " + countFT);
        sb.append("Total cassandra orphans in field_value ------------> " + fvOrphan);
        sb.append("Total cassandra rows in field_value ---------------> " + countFV);
        sb.append("\n\n***NOTE : Make sure (1)+(2)=(3) and (4)+(5)=(6)");

        String fileName = "results_" + (new SimpleDateFormat("YYYYMMddhhmmss").format(new Date())) + ".txt";
        File file = new File(fileName);
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        try {
            writer.write(sb.toString());
        } finally {
            if (writer != null) writer.close();
            System.out.println("***Results have been written to file  " + fileName);
        }
    }

    public static Map<Long, Long> getThingFieldMap() throws SQLException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        Connection conn = initMysqlJDBCDrivers();

        java.sql.ResultSet rs = null;
        if(conn != null && database.equals("mysql")){
            rs = conn.createStatement().executeQuery("SELECT id, thing_id, thingTypeFieldId FROM thingfield");
        }else if (conn != null && database.equals("mssql")){
            rs = conn.createStatement().executeQuery("SELECT id, thing_id, thingTypeFieldId FROM dbo.thingfield");
        }
        Map<Long, Long> thingFieldMap2 = new HashMap<Long, Long>();

        if(rs != null) {
            int counter  = 0 ;
            while (rs.next()) {

                Long thingId = rs.getLong("thing_id");
                Long thingFieldId = rs.getLong("id");

                thingFieldMap2.put(thingFieldId, thingId);
                counter++;
                if(counter % 10000 == 0){
                    System.out.print("\rRetrieving data from thingFields " + car[(counter/10000)%4]);
                }
            }
            System.out.println("\rRetrieving data from thingFields [OK]");
            conn.close();
        }else{
            System.out.println("No connection available for " + System.getProperty("connection.url." + database));
        }


        return thingFieldMap2;
    }

    public static Connection initMysqlJDBCDrivers() throws ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException {

        String url = System.getProperty("connection.url");
        String userName = System.getProperty("connection.username." + database);
        String password = System.getProperty("connection.password." + database);

        String driverMysql = "org.gjt.mm.mysql.Driver";
        String driverMssql = "net.sourceforge.jtds.jdbc.Driver";

        Class.forName(driverMysql).newInstance();
        Class.forName(driverMssql).newInstance();
        return DriverManager.getConnection(url, userName, password);
    }

    public static void setDBPrperties(){
        System.getProperties().put("connection.url", "jdbc:mysql://localhost:3306/riot_main");
        System.getProperties().put("connection.username.mysql", "root");
        System.getProperties().put("connection.password.mysql", "control123!");
        System.getProperties().put("connection.username.mssql", "sa");
        System.getProperties().put("connection.password.mssql", "control123!");

    }

    public static void main (String[] args){
        if(args.length == 0){
            System.out.print("Please send database as parameter (ie mysql or mssql)");
            System.exit(0);
        }
        try {

            database = args[0];
            setDBPrperties();
            verify();
            System.exit(0);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

}
