package com.snp.bd.bkyj.job;

import com.snp.bd.bkyj.model.TaskCondition;
import com.snp.bd.bkyj.rule.DBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by x on 2018/8/3.
 */
public class JobService {
    public static int JOB_STATE_RUNNING=3;
    public static int JOB_STATE_SUCCESS=4;
    public static int JOB_STATE_ERROR=5;
    private static Logger logger = LoggerFactory.getLogger(JobService.class);
    public static void updateJobState(int id,int state) throws Exception{
        Connection conn =  DBFactory.getInstance().getConnection();
        String sql="UPDATE WJ_TASK_CONDITION SET STATUS = ? WHERE ID = ?";
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
            try {
                ps.setInt(2,id);
                ps.setInt(1,state);
                ps.execute();
                conn.commit();
            } finally {
                ps.close();
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            DBFactory.close(null,ps,conn);
        }
    }
    public static List<TaskCondition> getTask(){
        List<TaskCondition> res=new ArrayList<>();
        String sql="select ID,CONDITION,TYPE from WJ_TASK_CONDITION where STATUS=1";
        Connection conn = null;
        try {
            conn = DBFactory.getInstance().getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(sql);
            try {
                rs = ps.executeQuery();
                try {
                    TaskCondition td;
                    while (rs.next()) {
                        td=new TaskCondition();
                        td.setID(rs.getInt("ID"));
                        td.setCONDITION(rs.getString("CONDITION"));
                        td.setTYPE(rs.getInt("TYPE"));
                        res.add(td);
                    }
                } finally {
                    rs.close();
                }
            } finally {
                ps.close();
            }
        } catch (SQLException e) {
        } finally {
            DBFactory.close(rs, ps, conn);
        }
        return res;
    }
}
