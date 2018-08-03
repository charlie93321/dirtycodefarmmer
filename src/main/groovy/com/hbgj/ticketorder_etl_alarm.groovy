package com.hbgj



import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import groovy.sql.Sql

import com.alibaba.fastjson.JSONObject;
import com.huoil.netutils.DefaultRequestSender
import com.huoil.netutils.Request
import com.huoli.utils.DateTimeUtil;

//读取配置脚本
def propsFile=new File("config/conf.properties");
def props = new Properties()
propsFile.withInputStream {  stream ->
    props.load(stream)
}

println "机票订单etl预警开始,"+DateFormatUtils.format(new Date(), "HH:mm:ss");
def ip="10.0.1.221"
def yesterday=DateTimeUtil.getYesterdayString()
def path="/home/ad_bigdata/data_job_workspace/order_etl/logs";
def cmd="grep error_msg etl.log|wc -l"
def result=['sh', '-c', cmd].execute(null,new File(path)).text
def count=Integer.parseInt(result.trim());
println " grep error_msg etl.log|wc -l :"+count;

def ant = new AntBuilder();
if(count>0){
//发送微信预警
    def requestSender =new DefaultRequestSender();
    request=new Request();
    request.setProperty("url","http://10.0.1.232/ad/v1/wx/report/alarm");
    request.setProperty("method","post");
    request.setProperty("encoding","utf-8");
    request.setParam("dep","3");
    request.setParam("agentid","1000004");
   // request.setParam("content",ip+" order_etl执行出现"+(count)+"个异常"+DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"));
    def resultSend=requestSender.sendRequest(request);
    println "发送个性推荐告警:"+result3;


}

System.exit(0);