package com.hbgj;
import com.jcraft.jsch.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public class JSchDemo {
    private String charset = "UTF-8"; // 设置编码格式
    private String user; // 用户名
    private String passwd; // 登录密码
    private String host; // 主机IP
    private JSch jsch;
    private Session session;

    /**
     *

     */
    public JSchDemo(String user, String passwd, String host) {
        this.user = user;
        this.passwd = passwd;
        this.host = host;
    }

    /**
     * 连接到指定的IP
     *
     * @throws JSchException
     */
    public void connect() throws JSchException {
        jsch = new JSch();
        session = jsch.getSession(user, host, 22);
        session.setPassword(passwd);
        java.util.Properties config = new java.util.Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);
        session.connect();
    }

    /**
     * 执行相关的命令
     */
    public String execCmd(String commandOrfilename,boolean isFile) {

        StringBuffer sb=new StringBuffer();
        String command = isFile?getCommand(commandOrfilename):commandOrfilename;

        //String command = "echo \"always\" >/sys/kernel/mm/transparent_hugepage/enabled ";
        BufferedReader reader = null;
        Channel channel = null;

        try {
            channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            channel.setInputStream(null);
            ((ChannelExec) channel).setErrStream(System.err);

            channel.connect();
            InputStream in = channel.getInputStream();
            reader = new BufferedReader(new InputStreamReader(in, Charset.forName(charset)));
            String buf = null;
            while ((buf = reader.readLine()) != null) {
                sb.append(buf).append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSchException e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            channel.disconnect();

        }

        return sb.toString();
    }

    public void releaseSession(){
        session.disconnect();
    }

    private String getCommand(String filename){
        StringBuffer sb=new StringBuffer();
        BufferedReader reader=null;
        try{
              reader=new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(filename)));
              String line=null;
              while((line=reader.readLine())!=null){
                  sb.append(line).append("\n");
              }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(reader!=null) {
                try {
                    reader.close();
                } catch (IOException e) {
                   e.printStackTrace();
                }
            }
        }


        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        String user="bigdata";
        String host="103.199.228.173";
        String passwd="sxljldh@1387";

        JSchDemo demo = new JSchDemo(user, passwd, host);
        demo.connect();
        demo.execCmd("test.sh",true);
        demo.releaseSession();
    }
}