import dto.Log;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Application {

    public static void main(String[] args) throws IOException, SQLException {




        Parser(args);

    }

    public static Connection connect(){
            System.out.println("Creating a connection..."); //Open a connection

            try{
                Class.forName("com.mysql.cj.jdbc.Driver").newInstance();

                Connection con= DriverManager.getConnection(
                        "jdbc:mysql://localhost/testdb?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC","root","password");

                return  con;

            }catch(Exception e){ System.out.println(e);}

            return null;

    }


    public static String Parser(String[] args) throws IOException, SQLException {
        DateTimeFormatter df = DateTimeFormat.forPattern("yyyy-MM-dd.HH:mm:ss");
        DateTimeFormatter df2 = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

        DateTime date = df.parseDateTime(args[1]);
        String duration = args[2];
        Integer threshold = Integer.parseInt(args[3]);
        File logFile = new File(args[0]);
        List<String> logs = new ArrayList<>();
        Scanner s = new Scanner(logFile);
        List<Log> logDtoList = new ArrayList<>();
        List<Log> logDtoListFiltered = new ArrayList<>();

        Connection connection = connect();
        Statement statement = connection.createStatement();
        String dbName = "testdb_"+date.toString();

        System.out.println("Parsing log file data.....");

        while (s.hasNextLine()){
            String log = s.nextLine();
            String[] logInfo = log.split("\\|");
            Log logDto = new Log();


            logDto.setDate(df2.parseDateTime(logInfo[0]));
            logDto.setIp(logInfo[1]);
            logDto.setRequest(logInfo[2]);
            logDto.setStatus(logInfo[3]);
            logDto.setUser(logInfo[4]);
            logDtoList.add(logDto);

        }

        System.out.println("Creating table in schema...");


        String createQuery = "CREATE TABLE `"+dbName+"` (\n" +
                "  `idlogs` int(11) NOT NULL AUTO_INCREMENT,\n" +
                "  `date` varchar(255) DEFAULT NULL,\n" +
                "  `ip` varchar(255) DEFAULT NULL,\n" +
                "  `request` varchar(255) DEFAULT NULL,\n" +
                "  `status` varchar(255) DEFAULT NULL,\n" +
                "  `user` varchar(255) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`idlogs`),\n" +
                "  UNIQUE KEY `idlogs_UNIQUE` (`idlogs`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=5145 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";

        statement.execute(createQuery);

        System.out.println("Inserting to database.....");

        Integer count = 0;
        for(Log log: logDtoList){

            String insertQuery = "insert into `"+dbName+"` (date, ip, request, status, user) values(\""+df2.print(log.getDate())+"\",\""+log.getIp()+"\","+log.getRequest()+",\""+log.getStatus()+"\","+log.getUser()+")";

            if(duration.equals("hourly")){
                if(log.getDate().isAfter(date) && log.getDate().isBefore(date.plusHours(1))){
                    System.out.println("Inserting log.... " + count++);
                    statement.execute(insertQuery);
                }
            }else if(duration.equals("daily")){
                if(log.getDate().isAfter(date) && log.getDate().isBefore(date.plusDays(1))){
                    System.out.println("Inserting log.... " + count++);
                    statement.execute(insertQuery);
                }
            }


        }

        statement.close();


        System.out.println("Querying from database.....");


        String queryFromDb = "select *, count(*) from `"+dbName+"` where (`date` BETWEEN ? AND ?) group by ip having count(*) >= ?;";
        PreparedStatement prepStmt = connection.prepareStatement(queryFromDb);
        DateFormat outputformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        if(duration.equals("hourly")){
            prepStmt.setString(1, df2.print(date));
            prepStmt.setString(2,df2.print(date.plusHours(1)));
            prepStmt.setInt(3, threshold);
            ResultSet returnFromQuery = prepStmt.executeQuery();

            while (returnFromQuery.next()){
                    String ip = returnFromQuery.getString("ip");

                    System.out.println(ip);
            }


        }else if(duration.equals("daily")){
            prepStmt.setString(1, df2.print(date));
            prepStmt.setString(2, df2.print(date.plusDays(1)));
            prepStmt.setInt(3, threshold);
            ResultSet returnFromQuery = prepStmt.executeQuery();

            while (returnFromQuery.next()){
                String ip = returnFromQuery.getString("ip");

                System.out.println(ip);
            }

        }

        prepStmt.close();




        return "OK";
    }

//    public static boolean checkDBExists(String dbName){
//
//        try{
//
//            Connection conn = connection();
//
//            ResultSet resultSet = conn.getMetaData().getCatalogs();
//
//            while (resultSet.next()) {
//
//                String databaseName = resultSet.getString(1);
//                if(databaseName.equals(dbName)){
//                    return true;
//                }
//            }
//            resultSet.close();
//
//        }
//        catch(Exception e){
//            e.printStackTrace();
//        }
//
//        return false;
//    }

//    public static boolean createDb(){
//        try {
//            Statement statement = connection().createStatement();
//            statement.executeUpdate("CREATE DATABASE test");
//            statement.close();
//            return true;
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//
//        return  false;
//    }



}

