package com.hbgj;

import scala.Tuple2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
    }


    static void execute(String sql,List<Tuple2<String,Object>> params){

        Connection connection=null;
        PreparedStatement pst=null;
        try {
            pst=connection.prepareStatement(sql);
            int index=0;
            for (int i = 0; i <params.size() ; i++) {
                Tuple2<String, Object> kv=params.get(i);

                String key=kv._1;
                Object value=kv._2;

                switch (key){
                    case "STRING":

                        try {
                               pst.setString(index++,value.toString());
                        }catch (Exception e) {
                            pst.setString(index++, "");
                        }
                        break;
                    case "INT":
                        try {
                            pst.setInt(index++, Integer.parseInt(value.toString()));
                        }catch (Exception e) {
                            pst.setInt(index++, -11111111);
                        }

                        break;

                    case "LONG":

                        try {
                            pst.setLong(index++,Long.parseLong(value.toString()));
                        }catch (Exception e) {
                            pst.setLong(index++,-11111111l);

                        }

                        break;
                    case "BOOLEAN":

                        try {
                            pst.setBoolean(index++,Boolean.parseBoolean(value.toString()));
                        }catch (Exception e) {
                            pst.setBoolean(index++,false);
                        }
                        break;
                    case "DOUBLE":

                        try {
                            pst.setDouble(index++,Double.parseDouble(value.toString()));
                        }catch (Exception e) {
                            pst.setDouble(index++,-11111111d);
                        }
                        break;
                    case "TIMESTAMP":
                        try {
                            pst.setTimestamp(index++,java.sql.Timestamp.valueOf(value.toString()));
                        }catch (Exception e) {
                            pst.setTimestamp(index++,java.sql.Timestamp.valueOf("1976-07-01 07:55:14.0"));
                        }
                        break;
                    default:
                        System.out.println(key+"is not in my play .............................................................");

                }


            }

            pst.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                pst.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }










    }
}
