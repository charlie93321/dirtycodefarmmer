package com.hbgj



/**
 * 常旅客卡告警分析
 */
import groovy.sql.Sql

import com.huoli.utils.DateTimeUtil
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import groovy.sql.Sql
import com.huoil.netutils.DefaultRequestSender
import com.huoil.netutils.Request



println "检查脱敏库是否正常开始,"+DateFormatUtils.format(new Date(), "HH:mm:ss");

//读取配置脚本
def propsFile=new File("config/conf.properties");
def props = new Properties()
propsFile.withInputStream {  stream ->
    props.load(stream)
}

try {

    def sql = Sql.newInstance(props['huoli.mysql.skyhotel.url'], props['huoli.mysql.skyhotel.user'], props['huoli.mysql.skyhotel.password'], props['huoli.mysql.skyhotel.driver']);

    def diff = 0;
    def time;

    def sqlQuery = "SELECT CREATETIME time,NOW(),TIMESTAMPDIFF(HOUR,CREATETIME,NOW()) diff FROM TICKET_ORDER ORDER BY CREATETIME DESC LIMIT 1";
    sql.eachRow(sqlQuery,{
        diff = it.diff;
        time = it.time;
    });
    sql.close();

    if(diff>=1){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","航班管家mysql脱敏数据库"+props['huoli.mysql.skyhotel.url']+" 表 TICKET_ORDER ORDER 超过一个小时没有数据，最后订单数据时间为"+DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss")+"，请查脱敏备库复制是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+result3;

    }

} catch(Exception e1) {


    def requestSender =new DefaultRequestSender();
    request=new Request();
    request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
    request.setProperty("method","post");
    request.setProperty("encoding","utf-8");
    request.setParam("dep","3");
    request.setParam("agentid","1000004");
    request.setParam("content","航班mysql数据库连接异常"+DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss")+"，请查脱敏备库是否正常");
    def result3=requestSender.sendRequest(request);
    println "发送个性推荐告警:"+result3;
}

try {
    def sqlgtgj = Sql.newInstance(props['huoli.mysql.gtgj.url'], props['huoli.mysql.gtgj.user'], props['huoli.mysql.gtgj.password'], props['huoli.mysql.gtgj.driver']);

    def diff1 = 0;
    def time1;

    sqlQuery = "SELECT CREATE_TIME time,NOW(),TIMESTAMPDIFF(HOUR,CREATE_TIME,NOW()) diff FROM gtgj.user_sub_order ORDER BY CREATE_TIME DESC LIMIT 1";
    sqlgtgj.eachRow(sqlQuery,{
        diff1 = it.diff;
        time1 = it.time;
    });

    if(diff1>=1){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","高铁管家mysql脱敏数据库"+props['huoli.mysql.gtgj.url']+" 表 gtgj.user_sub_order 超过一个小时没有数据，最后订单数据时间为"+DateFormatUtils.format(time1, "yyyy-MM-dd HH:mm:ss")+"，请查脱敏备库复制是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+result3;
    }

    sqlQuery = "SELECT create_time time,NOW(),TIMESTAMPDIFF(HOUR,create_time,NOW()) diff FROM gtgj.platformbuy_sub_orders ORDER BY create_time DESC LIMIT 1";
    sqlgtgj.eachRow(sqlQuery,{
        diff1 = it.diff;
        time1 = it.time;
    });

    if(diff1>=1){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","高铁管家mysql脱敏数据库"+props['huoli.mysql.gtgj.url']+" 表 gtgj.platformbuy_sub_orders 超过一个小时没有数据，最后订单数据时间为"+DateFormatUtils.format(time1, "yyyy-MM-dd HH:mm:ss")+"，请查脱敏备库复制是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+result3;
    }

    sqlgtgj.close();

} catch(Exception e1) {


    def requestSender =new DefaultRequestSender();
    request=new Request();
    request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
    request.setProperty("method","post");
    request.setProperty("encoding","utf-8");
    request.setParam("dep","3");
    request.setParam("agentid","1000004");
    request.setParam("content","高铁mysql数据库连接异常"+DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss")+"，请查脱敏备库是否正常");
    def result3=requestSender.sendRequest(request);
    println "发送个性推荐告警:"+result3;
}

String url = props['huoli.hive.default.url']
String user = props['huoli.hive.default.user']
String password = props['huoli.hive.default.password']
String driverClassName = props['huoli.hive.default.driver']
try {
    Sql hiveJdbc = Sql.newInstance(url, user, password, driverClassName);
    hiveJdbc.close();
} catch(Exception e1) {
    def requestSender1 =new DefaultRequestSender();
    request=new Request();
    request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
    request.setProperty("method","post");
    request.setProperty("encoding","utf-8");
    request.setParam("dep","3");
    request.setParam("agentid","1000004");
    request.setParam("content","Hive数据仓库连接异常"+DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss")+"，请查HIVE数据仓库是否正常");
    def resultSend1=requestSender1.sendRequest(request);
    println "发送个性推荐告警:"+resultSend;
}

System.exit(0);
