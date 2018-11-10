package com.snp.bd.bkyj.util;

import ch.ethz.ssh2.*;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 *  Description: SSH连接目标主机执行相关操作工具类
 *  Author: h
 *  Date：2017-03-20
 */
public class RemoteConnection {
	
	private static Logger logger = LoggerFactory.getLogger(RemoteConnection.class);
	private String hostName;
	private String user;
	private String passwd;
	private boolean hasError = false;
    private String ErrorMessage = "";
    private boolean isSuccessfully = false;
    private String SystemMessage = "";


	public static void main(String[] args) {
		RemoteConnection rc = null;

			rc =new RemoteConnection(Config.SUBMIT_HOST,Config.SUBMIT_HOST_USER,Config.SUBMIT_PASSWORD);
		System.out.println("rc = " + rc.execCommand("hadoop fs -ls /"));//echo 'echo $SPARK_HOME > /test.txt' > /cmd.sh && sh cmd.sh

	}


	public RemoteConnection(String hostName, String user, String passwd){
		this.hostName=hostName;
		this.user=user;
		this.passwd=passwd;
	}
	
	/*
	 * 使用SSH方式连接远程主机
	 */
	private Connection connection(){
		//指明连接主机的IP地址  
        Connection conn = new Connection(this.hostName);
        try {
            //连接到主机  
            conn.connect();
            //使用用户名和密码校验  
            boolean isconn = conn.authenticateWithPassword(this.user, this.passwd);
            if (!isconn) 
            {
                logger.warn("用户名称或密码不正确");
                System.out.println("用户名称或密码不正确");
                return null;
            } 
            else 
            {
				logger.info("成功连接远程主机" + this.hostName);
				System.out.println("成功连接远程主机" + this.hostName);
                return conn;
            }            
        } catch (Exception e) 
        {
			logger.warn("SSH方式连接远程主机" + this.hostName + "出现异常!");
			logger.warn(e.getMessage());
			System.out.println(e.getMessage());
        	e.printStackTrace();
            return null;
        } 
	}
	
	 /** 
     * 关闭连接 
     */  
    private void disconnect(Connection conn, Session ssh){  
        try{  
        	if(ssh!=null){
        		ssh.close();
        	}
        	if(conn!=null){
        		conn.close();
        	}
        }catch(Exception e){
			logger.warn("关闭连接时出现异常!");
			logger.warn(e.getMessage());
        	e.printStackTrace();
        }  
     }  
       
	
	/*
	 * 成功连接远程主机后，执行linux命令或shell脚本
	 * @param cmdStr: cmdStr为待执行的命令（包括单条命令和执行shell脚本的命令，多条命令之间以;相隔）
	 * //只允许使用一行命令，即ssh对象只能使用一次execCommand这个方法，多次使用则会出现异常，使用多个命令用分号隔开 
	 */
	public boolean execCommand(String cmdStr){
		Connection conn = connection();
		if(conn!=null){
			Session ssh = null;
	        try {
	        		ssh = conn.openSession();
					ssh.execCommand(cmdStr);
					System.out.println(cmdStr);
					//将Terminal屏幕上的文字全部打印出来
					InputStream is = new StreamGobbler(ssh.getStdout());
					BufferedReader brs = new BufferedReader(new InputStreamReader(is));
					while (true) {
						String line = brs.readLine();
						if (line == null) {
							break;
						}
						System.out.println(line);
					}
                return true;
            }catch (IOException e) 
            {
				logger.warn("执行linux命令或shell脚本时出现异常!");
            	e.printStackTrace();
            	return false;
            } finally
	        {
            	disconnect(conn,ssh);
	        }
		}
		return false;
	}
		
	
	/*
	 * 连接远程主机后，从远程主机获取文件到本地
	 * @param remoteFiles:远程主机要复制的文件（包含其所在路径）
	 * @param localDirectory：文件拷贝到本地后存放的路径
	 */
	public boolean copyFileToLocal(String remoteFiles, String localDirectory){
		Connection conn = connection();
		if(conn!=null){
			try{
				 SCPClient scpClient = conn.createSCPClient();
				 scpClient.get(remoteFiles,localDirectory);
	        } catch (Exception e)
			{
				logger.warn("从远程主机获取文件到本地时出现异常!");
            	e.printStackTrace();
            	return false;
            } finally 
	        {
            	disconnect(conn,null);
	        }
			return true;
		}
		return false;
	}
	
	/*
	 * 连接远程主机后，从本地复制文件到远程目录
	 * @param localFiles:本地要复制的文件（包含其所在路径）
	 * @param remoteDirectory：文件拷贝到远程主机后存放的路径
	 */
	public boolean copyFileFromLocal(String[] localFiles, String remoteDirectory){
		Connection conn = connection();
		if(conn!=null){
			try{
				SCPClient scpClient = conn.createSCPClient(); 
	            scpClient.put(localFiles,remoteDirectory);
	        } catch (Exception e) 
			{
				logger.warn("从本地复制文件到远程目录时出现异常!");
            	e.printStackTrace();
            	return false;
            } finally 
	        {
            	disconnect(conn,null);
	        }
			return true;
		}
		return false;
	}


	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getPasswd() {
		return passwd;
	}
	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}
	public boolean isHasError() {
		return hasError;
	}
	public void setHasError(boolean hasError) {
		this.hasError = hasError;
	}
	public String getErrorMessage() {
		return ErrorMessage;
	}
	public void setErrorMessage(String errorMessage) {
		ErrorMessage = errorMessage;
	}
	public boolean isSuccessfully() {
		return isSuccessfully;
	}
	public void setSuccessfully(boolean isSuccessfully) {
		this.isSuccessfully = isSuccessfully;
	}
	public String getSystemMessage() {
		return SystemMessage;
	}
	public void setSystemMessage(String systemMessage) {
		SystemMessage = systemMessage;
	}
}
