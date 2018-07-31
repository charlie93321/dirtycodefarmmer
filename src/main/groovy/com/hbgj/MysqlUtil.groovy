package com.hbgj

import org.apache.log4j.Logger
import scala.Tuple2

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException






class MysqlUtil{
    /**
     *
     *
     * jdbc:mysql://hd1:3306/hbgjdb  hbgj hbgj
     * jdbc:mysql://47.93.56.114:3306/etl                              adseretl/4ee4dfgfdealK
     * */
    private static  final String DRIVER="com.mysql.jdbc.Driver"
    private static  final  String URL="jdbc:mysql://47.93.56.114:3306/etl"
    private static  final  String USERNAME="adseretl"
    private static  final  String PASSWORD="4ee4dfgfdealK"



    private static  Connection connection=null
    private static   final Logger LOGGER=Logger.getLogger(MysqlUtil.class)
    static{

        Class.forName(DRIVER)
        LOGGER.debug("连接数据库...")
        try {
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)
            LOGGER.debug(" 获取数据库连接成功")
        }catch (Exception e){
            LOGGER.debug(" 获取数据库连接失败",e)
        }

    }




    static def execute(sql, params){



        PreparedStatement pst=null
        try {
            pst=connection.prepareStatement(sql)

            int index=1
            for (int i = 0; i <params.size() ; i++) {
                Tuple2<String, Object> kv=params.get(i);
                String key=kv._1
                Object value=kv._2
                switchSet(index,key,value,pst)
                index++
            }
            pst.execute()
        } catch (SQLException e) {
            e.printStackTrace()
        }finally {
            try {
                pst.close();
            } catch (SQLException e) {
                e.printStackTrace()
            }
        }
    }


    static def executeBach(sql, params, bach){



        PreparedStatement pst=null
        try {
            pst=connection.prepareStatement(sql)

            for (int bachIndex = 0; bachIndex <params.size()/bach ; bachIndex++) {

                int index=1
                for (int j = 0; j <bach ; j++) {
                    Tuple2<String, Object> kv=params.get(bachIndex*bach+j)
                    String key=kv._1
                    Object value=kv._2
                    switchSet(index,key,value,pst)
                    index++
                }
                pst.addBatch()
            }
            pst.executeBatch()
        } catch (SQLException e) {
            e.printStackTrace()
        }finally {
            try {
                pst.close();
            } catch (SQLException e) {
                e.printStackTrace()
            }
        }
    }

    static def executeQuery(sql, params, fields,filename){



        PreparedStatement pst=null
        ResultSet rs=null
        try {
            pst=connection.prepareStatement(sql)

            int index=1
            for (int i = 0; i <params.size() ; i++) {
                Tuple2<String, Object> kv=params.get(i);
                String key=kv._1
                Object value=kv._2
                switchSet(index,key,value,pst)
                index++
            }
            rs=pst.executeQuery()


            File wf=new File("D:\\zxy\\tempdir\\"+filename)
            while(rs.next()){
                def sb=new StringBuffer()
                for (int i = 1; i <=fields; i++) {
                    def r=rs.getObject(i).toString()
                    if(i!=fields)
                        sb.append(r).append("|")
                    else
                         sb.append(r).append("\n")
                }
                wf.append(sb.toString())

            }

        } catch (SQLException e) {
            e.printStackTrace()
        }finally {
            try {
                pst.close();
            } catch (SQLException e) {
                e.printStackTrace()
            }
        }
    }

    def static   release(){
        connection.close()
    }

    def static switchSet(index,key,value,pst){
        switch (key){
            case "STRING":

                try {
                    pst.setString(index,value.toString());
                }catch (Exception e) {
                    pst.setString(index, "");
                }

                break;
            case "INT":
                try {
                    pst.setInt(index, Integer.parseInt(value.toString()));
                }catch (Exception e) {
                    pst.setInt(index, -11111111);
                }

                break;

            case "LONG":

                try {
                    pst.setLong(index,Long.parseLong(value.toString()));
                }catch (Exception e) {
                    pst.setLong(index,-11111111l);

                }

                break;
            case "BOOLEAN":

                try {
                    pst.setBoolean(index,Boolean.parseBoolean(value.toString()));
                }catch (Exception e) {
                    pst.setBoolean(index,false);
                }
                break;
            case "DOUBLE":

                try {
                    pst.setDouble(index,Double.parseDouble(value.toString()));
                }catch (Exception e) {
                    pst.setDouble(index,-11111111d);
                }
                break;
            case "TIMESTAMP":
                try {
                    pst.setTimestamp(index,java.sql.Timestamp.valueOf(value.toString()));
                }catch (Exception e) {
                    pst.setTimestamp(index,java.sql.Timestamp.valueOf("1976-07-01 07:55:14.0"))
                }
                break;
            default:
                System.out.println(key+"is not in my play .............................................................");

        }
    }




     static void main(String[] args){

       List<Tuple2<String,Object>> params=new ArrayList<>();


         File ids=new File("D:\\zxy\\worknote\\201807\\thread_p1\\GroovyJscala\\src\\main\\resources\\temp2")

         ids.eachLine("utf-8",{
             def arr=it.split("\\|",-1)
             // id  province city
             params.add(new scala.Tuple2<String, Object>("STRING", arr[0]))
             params.add(new scala.Tuple2<String, Object>("STRING", arr[1]))
             params.add(new scala.Tuple2<String, Object>("STRING", arr[2]))
         })


        //List<Tuple2<String,Object>> params=new ArrayList<>();


        // MysqlUtil.executeQuery("select * from   card_id_city ",params,3,"card_id_city")

         MysqlUtil.executeBach("insert into  card_id_city values (?,?,?)  ",params,3)

        MysqlUtil.release()
    }

    static String getProvince(List<String> ps, String area) {
        String provice=null
        for (int i = 0; i <ps.size() ; i++) {
            def p=ps.get(i)
            if(area.contains(p)){
                provice= p
            }
        }
         provice
    }
}


