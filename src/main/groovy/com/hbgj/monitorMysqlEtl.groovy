package com.hbgj

/**
 * mysql etl告警分析
 */
import groovy.sql.Sql

import com.huoli.utils.DateTimeUtil
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import groovy.sql.Sql

import com.alibaba.fastjson.JSONObject;
import com.huoil.netutils.DefaultRequestSender
import com.huoil.netutils.Request
import com.huoli.utils.DateTimeUtil;


println "检查etl库是否正常开始,"+DateFormatUtils.format(new Date(), "HH:mm:ss");

//读取配置脚本
def propsFile=new File("config/conf.properties");
def props = new Properties()
propsFile.withInputStream {  stream ->
    props.load(stream)
}



try {

    def sql = Sql.newInstance(props['huoli.mysql.etl.url'], props['huoli.mysql.etl.user'], props['huoli.mysql.etl.password'], props['huoli.mysql.etl.driver']);

    def diff = 0
    def time

//areaby_ip
    def sqlQuery = "SELECT update_time TIME,NOW(),TIMESTAMPDIFF(HOUR,update_time,NOW()) diff FROM information_schema.TABLES WHERE TABLE_SCHEMA='etl' AND information_schema.TABLES.TABLE_NAME = 'areaby_ip'";
    sql.eachRow(sqlQuery,{
        diff = it.diff;
        time = it.time;
    });


    if(diff>=25){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","ETL mysql数据库"+props['huoli.mysql.etl.url']+"表 areaby_ip 超过一天没有生成数据，最后生成数据时间为"+DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss")+"，请查脚本运行是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+"areaby_ip";

    }



//tag_car_order_info
// sqlQuery = "SELECT create_time TIME,NOW(),TIMESTAMPDIFF(HOUR,create_time,NOW()) diff FROM information_schema.TABLES WHERE TABLE_SCHEMA='etl' AND information_schema.TABLES.TABLE_NAME = 'tag_car_order_info'";
//sql.eachRow(sqlQuery,{
//        diff = it.diff;
//        time = it.time;
//});


//if(diff>=25){
//        def requestSender =new DefaultRequestSender();
//        request=new Request();
//        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
//        request.setProperty("method","post");
//        request.setProperty("encoding","utf-8");
//        request.setParam("dep","3");
//        request.setParam("agentid","1000004");
//        request.setParam("content","ETL mysql数据库"+props['huoli.mysql.etl.url']+"表 tag_car_order_info 超过一天没有生成数据，最后生成数据时间为"+DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss")+"，请查脚本运行是否正常");
//        def result3=requestSender.sendRequest(request);
//        println "发送个性推荐告警:"+"tag_car_order_info ";

//}

//ad_trace_click
    sqlQuery = "SELECT dt TIME,NOW(),TIMESTAMPDIFF(HOUR,dt,NOW()) diff FROM etl.ad_trace_click ORDER BY dt DESC LIMIT 1";
    sql.eachRow(sqlQuery,{
        diff = it.diff;
        time = it.time;
    });


    if(diff>57){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","ETL mysql数据库"+props['huoli.mysql.etl.url']+"表 ad_trace_click 超过一天没有生成数据，最后生成数据时间为"+ time +"，请查脚本运行是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+"ad_trace_click";

    }


//tag_gt_order_info
// sqlQuery = "SELECT create_time TIME,NOW(),TIMESTAMPDIFF(HOUR,create_time,NOW()) diff FROM information_schema.TABLES WHERE TABLE_SCHEMA='etl' AND information_schema.TABLES.TABLE_NAME = 'tag_gt_order_info'";
//sql.eachRow(sqlQuery,{
    //      diff = it.diff;
    //      time = it.time;
//});


//if(diff>=25){
//        def requestSender =new DefaultRequestSender();
//        request=new Request();
//        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
//        request.setProperty("method","post");
//        request.setProperty("encoding","utf-8");
//        request.setParam("dep","3");
//        request.setParam("agentid","1000004");
//        request.setParam("content","ETL mysql数据库"+props['huoli.mysql.etl.url']+"表 tag_gt_order_info 超过一天没有生成数据，最后生成数据时间为"+DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss")+"，请查脚本运行是否正常");
//        def result3=requestSender.sendRequest(request);
//        println "发送个性推荐告警:"+"tag_gt_order_info";
//}

//push_inter_search_new_user
    sqlQuery = "SELECT insert_time TIME,NOW(),TIMESTAMPDIFF(HOUR,insert_time,NOW()) diff FROM etl.push_inter_search_new_user ORDER BY insert_time DESC LIMIT 1 ";
    sql.eachRow(sqlQuery,{
        diff = it.diff;
        time = it.time;
    });


    if(diff>=25){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","ETL mysql数据库"+props['huoli.mysql.etl.url']+"表 push_inter_search_new_user 超过一天没有生成数据，最后生成数据时间为"+DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss")+"，请查脚本运行是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+"push_inter_search_new_user";

    }

//pfrom_hbgj_channel_day
    sqlQuery = "SELECT dt TIME,NOW(),TIMESTAMPDIFF(HOUR,dt,NOW()) diff FROM etl.pfrom_hbgj_channel_day ORDER BY dt DESC LIMIT 1 ";
    sql.eachRow(sqlQuery,{
        diff = it.diff;
        time = it.time;
    });


    if(diff>57){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","ETL mysql数据库"+props['huoli.mysql.etl.url']+"表 pfrom_hbgj_channel_day 超过一天没有生成数据，最后生成数据时间为"+time+"，请查脚本运行是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+"pfrom_hbgj_channel_day";

    }

//hb_dynamicinfo_day
    sqlQuery = "SELECT dt TIME,NOW(),TIMESTAMPDIFF(HOUR,dt,NOW()) diff FROM etl.hb_dynamicinfo_day ORDER BY dt DESC LIMIT 1 ";
    sql.eachRow(sqlQuery,{
        diff = it.diff;
        time = it.time;
    });


    if(diff>57){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","ETL mysql数据库"+props['huoli.mysql.etl.url']+"表 hb_dynamicinfo_day 超过一天没有生成数据，最后生成数据时间为"+time+"，请查脚本运行是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+"hb_dynamicinfo_day";

    }

//push_35
    sqlQuery = "SELECT createtime TIME,NOW(),TIMESTAMPDIFF(HOUR,createtime,NOW()) diff FROM etl.push_35 ORDER BY createtime DESC LIMIT 1";
    sql.eachRow(sqlQuery,{
        diff = it.diff;
        time = it.time;
    });


    if(diff>=25){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","ETL mysql数据库"+props['huoli.mysql.etl.url']+"表 push_35 超过一小时没有生成数据，最后生成数据时间为"+DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss")+"，请查脚本运行是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+"push_35 ";

    }

//apisearch_history
    sqlQuery = "SELECT createtime TIME,NOW(),TIMESTAMPDIFF(HOUR,createtime,NOW()) diff FROM etl.apisearch_history ORDER BY createtime DESC LIMIT 1 ";
    sql.eachRow(sqlQuery,{
        diff = it.diff;
        time = it.time;
    });


    if(diff>=25){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","ETL mysql数据库"+props['huoli.mysql.etl.url']+"表 apisearch_history 超过一小时没有生成数据，最后生成数据时间为"+DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss")+"，请查脚本运行是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+"apisearch_history";

    }



//tag_gtgj_baseinfo
    sqlQuery = "SELECT create_time TIME,NOW(),TIMESTAMPDIFF(HOUR,create_time,NOW()) diff FROM information_schema.TABLES WHERE TABLE_SCHEMA='etl' AND information_schema.TABLES.TABLE_NAME = 'tag_gtgj_baseinfo'";
    sql.eachRow(sqlQuery,{
        diff = it.diff;
        time = it.time;
    });


    if(diff>=175){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","ETL mysql数据库"+props['huoli.mysql.etl.url']+"表 tag_gtgj_baseinfo 本周没有生成数据，最后生成数据时间为"+DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss")+"，请查脚本运行是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+"tag_gtgj_baseinfo";

    }


//portrait_base_info
    sqlQuery = "SELECT create_time TIME,NOW(),TIMESTAMPDIFF(HOUR,create_time,NOW()) diff FROM information_schema.TABLES WHERE TABLE_SCHEMA='etl' AND information_schema.TABLES.TABLE_NAME = 'portrait_base_info'";
    sql.eachRow(sqlQuery,{
        diff = it.diff;
        time = it.time;
    });


    if(diff>=175){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","ETL mysql数据库"+props['huoli.mysql.etl.url']+"表 portrait_base_info 本周没有生成数据，最后生成数据时间为"+DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss")+"，请查脚本运行是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+"portrait_base_info";

    }

//portrait_order_info
    sqlQuery = "SELECT create_time TIME,NOW(),TIMESTAMPDIFF(HOUR,create_time,NOW()) diff FROM information_schema.TABLES WHERE TABLE_SCHEMA='etl' AND information_schema.TABLES.TABLE_NAME = 'portrait_order_info'";
    sql.eachRow(sqlQuery,{
        diff = it.diff;
        time = it.time;
    });


    if(diff>=175){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","ETL mysql数据库"+props['huoli.mysql.etl.url']+"表 portrait_order_info 本周没有生成数据，最后生成数据时间为"+DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss")+"，请查脚本运行是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+"portrait_order_info";

    }

//tag_base_info
    sqlQuery = "SELECT create_time TIME,NOW(),TIMESTAMPDIFF(HOUR,create_time,NOW()) diff FROM information_schema.TABLES WHERE TABLE_SCHEMA='etl' AND information_schema.TABLES.TABLE_NAME = 'tag_base_info'";
    sql.eachRow(sqlQuery,{
        diff = it.diff;
        time = it.time;
    });


    if(diff>=175){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","ETL mysql数据库"+props['huoli.mysql.etl.url']+"表 tag_base_info 本周没有生成数据，最后生成数据时间为"+DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss")+"，请查脚本运行是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+"tag_base_info ";

    }

//resident_city
    sqlQuery = "SELECT create_time TIME,NOW(),TIMESTAMPDIFF(HOUR,create_time,NOW()) diff FROM information_schema.TABLES WHERE TABLE_SCHEMA='etl' AND information_schema.TABLES.TABLE_NAME = 'resident_city'";
    sql.eachRow(sqlQuery,{
        diff = it.diff;
        time = it.time;
    });


    if(diff>=175){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","ETL mysql数据库"+props['huoli.mysql.etl.url']+"表 resident_city 本周没有生成数据，最后生成数据时间为"+DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss")+"，请查脚本运行是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+"resident_city ";

    }

//report_sys.ad_data_report
    sqlQuery = "SELECT CREATE_TIME time,NOW(),TIMESTAMPDIFF(HOUR,CREATE_TIME,NOW()) diff FROM report_sys.ad_data_report ORDER BY CREATE_TIME DESC LIMIT 1";
    sql.eachRow(sqlQuery,{
        diff = it.diff;
        time = it.time;
    });

    if(diff>=1){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","ETL mysql数据库"+props['huoli.mysql.etl.url']+"表 report_sys.ad_data_report 超过一小时没有生成数据，最后生成数据时间为"+DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss")+"，请查脚本运行是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+"report_sys.ad_data_report";

    }




//analysesourceentry_all
    sqlQuery = "SELECT date TIME,NOW(),TIMESTAMPDIFF(HOUR,date,NOW()) diff FROM sstats.analysesourceentry_all ORDER BY date DESC LIMIT 1 ";
    sql.eachRow(sqlQuery,{
        diff = it.diff;
        time = it.time;
    });


    if(diff>57){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","ETL mysql数据库"+props['huoli.mysql.etl.url']+"表 analysesourceentry_all 昨天数据没有生成，最后生成数据时间为"+time+"，请查脚本运行是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+"analysesourceentry_all";

    }



//push_new_user_gt
    sqlQuery = "SELECT create_time TIME,NOW(),TIMESTAMPDIFF(HOUR,create_time,NOW()) diff FROM etl.push_new_user_gt ORDER BY create_time DESC LIMIT 1";
    sql.eachRow(sqlQuery,{
        diff = it.diff;
        time = it.time;
    });


    if(diff>=1){
        def requestSender =new DefaultRequestSender();
        request=new Request();
        request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
        request.setProperty("method","post");
        request.setProperty("encoding","utf-8");
        request.setParam("dep","3");
        request.setParam("agentid","1000004");
        request.setParam("content","ETL mysql数据库"+props['huoli.mysql.etl.url']+"表 push_new_user_gt 超过一小时没有生成数据，最后生成数据时间为"+DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss")+"，请查脚本运行是否正常");
        def result3=requestSender.sendRequest(request);
        println "发送个性推荐告警:"+"push_new_user_gt ";
    }


    sql.close();


} catch(Exception e1) {


    def requestSender =new DefaultRequestSender();
    request=new Request();
    request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
    request.setProperty("method","post");
    request.setProperty("encoding","utf-8");
    request.setParam("dep","3");
    request.setParam("agentid","1000004");
    request.setParam("content","monitorMysqlEtl.groovy 脚本异常"+DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss")+e1);
    def result3=requestSender.sendRequest(request);
    println "发送个性推荐告警:"+e1;
}




System.exit(0);