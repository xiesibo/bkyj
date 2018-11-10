package com.snp.bd.bkyj.rule;

import com.snp.bd.bkyj.model.*;
import org.apache.commons.digester.Rule;
import org.apache.commons.digester.Rules;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.ivy.util.StringUtils;
import org.apache.spark.network.protocol.Encoders;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wxh-pc on 2017/9/5.
 */
public class RuleService implements Serializable {


    private long sync_period = 20000; //ms   每5秒同步一次
    private long last_fetch_time = 0;
    private List<WarningRuleModel> rules;  //规则
    private List<SupectModel> supects;  //线索
    private List<KeyAreaModel> areas;  //区域信息
    private List<CaseInfoModel> cases; //案件信息
    private Map<String, WbModel> wbInfos;//网吧信息
    private List<YxwjModel> yxwjRules;
    public static RuleService getInstance() {
        return DataServiceHolder.instance;
    }

    private static class DataServiceHolder {
        private static RuleService instance = new RuleService();
    }

    private RuleService() {

    }
    /**
     * 获取所有禁毒案件
     * @return
     */
    public  List<Integer> getJdAj(){
        String sql="SELECT c.\"ID\" FROM WJ_CASEINFO c INNER JOIN ( SELECT \"ID\" FROM WJ_NATURE WHERE CONNECT_BY_ISLEAF = 1 START WITH \"NAME\" = '禁毒案件' CONNECT BY PARENTID = PRIOR \"ID\" ) tem ON tem.\"ID\" = c.\"NATURESUB\"";
        List<Integer> jdaj=new ArrayList<>();
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
                    while (rs.next()) {
                        jdaj.add(rs.getInt("ID"));
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
        return jdaj;
    }
//    /**
//     * 分析结果写入数据库  单个写入
//     * @param point
//     */
//    public void writeResult2DB(TrackPoint point){
//
//        if(point == null) return;
//        String sql = "insert into t_track_point(name,lat,lng,jgsj) values(?,?,?,?)";
//
//        Connection conn = DBFactory.getConnection();
//        if(conn == null) return;
//
//        PreparedStatement ps = null;
//
//        try {
//            ps = conn.prepareStatement(sql);
//            try {
//                ps.setString(1, point.getName());
//                ps.setFloat(2,point.getLat());
//                ps.setFloat(3,point.getLng());
//                ps.setTimestamp(4, new Timestamp(point.getJgsj().getTime()));
//                ps.execute();
//            } finally {
//                ps.close();
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } finally {
//            DBFactory.close(null,ps,conn);
//        }
//
//    }
//
//
//    /**
//     * 将分析结果写入到库
//     * @param points
//     */
//    public void writeResult2DB(List<TrackPoint> points){
//
//        if(points == null || points.size() == 0) return;
//        String sql = "insert into t_track_point(name,lat,lng,jgsj) values(?,?,?,?)";
//
//        Connection conn = DBFactory.getConnection();
//        if(conn == null) return;
//
//        PreparedStatement ps = null;
//
//        try {
//            ps = conn.prepareStatement(sql);
//            try {
//                for (TrackPoint data : points) {
//                    ps.setString(1, data.getName());
//                    ps.setFloat(2,data.getLat());
//                    ps.setFloat(3,data.getLng());
//                    ps.setTimestamp(4, new Timestamp(data.getJgsj().getTime()));
//                    ps.addBatch();
//                }
//                ps.executeBatch();
//            } finally {
//                ps.close();
//            }
//        } catch (SQLException e) {
//           e.printStackTrace();
//        } finally {
//            DBFactory.close(null,ps,conn);
//        }
//
//    }
//
//
//
//    /**
//     * 從文本中加載關注的數據對象
//     * @throws Exception
//     */
//    public List<String> getInfoFromFile(){
//        List<String> list = new ArrayList<String>();
//
//        try {
//            BufferedReader br = new BufferedReader(new FileReader(new File("data/importps.txt")));
//            String str = br.readLine();
//            while (str != null) {
//                list.add(str);
//                str = br.readLine();
//            }
//            br.close();
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        return list;
//    }

//

    /***
     * 根据线索id列表获取线索
     *
     * @param ids
     * @return
     */
    public List<SupectModel> getSupectsByIds(String[] ids) {
        List<SupectModel> supects1 = new ArrayList<SupectModel>();
        if (ids == null) return supects1;
        List<SupectModel> r = getSupects();
        if (r == null) return supects1;
        for (String id : ids) {
            for (SupectModel sm : r) {
                try {
                    if (id.length()>0&&sm.getId()!=null&&id.equals(String.valueOf(sm.getId()))) {
                        supects1.add(sm);
                        break;
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        return supects1;
    }


    /***
     * 根据区域id列表获取区域
     *
     * @param ids
     * @return
     */
    public List<KeyAreaModel> getAreasByIds(String[] ids) {
        List<KeyAreaModel> areas1 = new ArrayList<KeyAreaModel>();
        if (ids == null) return areas1;
        List<KeyAreaModel> r = getAreas();
        if (r == null) return areas1;
        for (String id : ids) {
            for (KeyAreaModel sm : r) {
                if (id.equals(String.valueOf(sm.getId()))) {
                    areas1.add(sm);
                    break;
                }
            }
        }
        return areas1;
    }

    /**
     * 根据案件ID获取案件信息
     *
     * @param id
     * @return
     */
    public CaseInfoModel getCaseInfoById(int id) {
        List<CaseInfoModel> cases = getCases();
        for (CaseInfoModel c : cases) {
            if (c.getId() == id) {
                return c;
            }
        }
        return null;
    }

    private long last_fetch_time_case = 0;

    /**
     * 同步案件信息
     *
     * @return
     */
    public List<CaseInfoModel> getCases() {

        if (cases != null) {
            if (System.currentTimeMillis() - last_fetch_time_case < sync_period) return cases;
        }

        ArrayList<CaseInfoModel> cases1 = new ArrayList<CaseInfoModel>();

        Connection conn = null;
        try {
            conn = DBFactory.getInstance().getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (conn == null) {
            return cases;
        }
        String sql = "select * from WJ_CASEINFO";
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(sql);
            try {
                rs = ps.executeQuery();
                try {
                    while (rs.next()) {

                        CaseInfoModel c = new CaseInfoModel();
                        c.setId(rs.getInt("ID"));
                        c.setName(rs.getString("NAME"));
                        c.setCode(rs.getString("CODE"));
                        c.setNature(rs.getInt("NATURE"));
                        c.setNature_sub(rs.getInt("NATURESUB"));
                        c.setRemark(rs.getString("REMARK"));
                        c.setFile_ids(rs.getString("FILEIDS"));
                        c.setInput_id(rs.getInt("INPUTERID"));
                        c.setCreate_time(rs.getDate("CREATEDATE"));
                        c.setStatus(rs.getInt("STATUS"));
                        c.setDescription(rs.getString("SECRETDESC"));
                        c.setUser_ids(rs.getString("USERIDS"));
                        cases1.add(c);
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
        last_fetch_time_case = System.currentTimeMillis();
        cases=cases1;
        return cases;
    }


    private long last_fetch_time_area = 0;

    /**
     * 同步区域信息
     *
     * @return
     */
    public List<KeyAreaModel> getAreas() {

        if (areas != null) {
            if (System.currentTimeMillis() - last_fetch_time_area < sync_period) return areas;
        }

        ArrayList<KeyAreaModel> areas1 = new ArrayList<KeyAreaModel>();

        Connection conn = null;
        try {
            conn = DBFactory.getInstance().getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (conn == null) {
            return areas;
        }
        String sql = "select * from WJ_KEY_AREA";
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(sql);
            try {
                rs = ps.executeQuery();
                try {
                    while (rs.next()) {

                        KeyAreaModel area = new KeyAreaModel();
                        area.setId(rs.getInt("ID"));
                        area.setName(rs.getString("NAME"));
                        area.setArea(rs.getString("AREA"));
                        area.setLog_time(rs.getDate("LOGTIME"));
                        area.setLog_user(rs.getInt("LOGUSER"));
                        area.setType(rs.getInt("TYPE"));
                        areas1.add(area);
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
        last_fetch_time_area = System.currentTimeMillis();
        areas=areas1;
        return areas;
    }

    private long last_fetch_time_supect = 0;

    /**
     * 同步线索信息规则
     *
     * @return
     */
    public List<SupectModel> getSupects() {

        if (supects != null) {
            if (System.currentTimeMillis() - last_fetch_time_supect < sync_period) return supects;
        }

        ArrayList<SupectModel> supects1 = new ArrayList<SupectModel>();

        Connection conn = null;
        try {
            conn = DBFactory.getInstance().getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (conn == null) {
            return supects;
        }
        String sql = "select * from WJ_SUPECT";
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(sql);
            try {
                rs = ps.executeQuery();
                try {
                    while (rs.next()) {

                        SupectModel supect = new SupectModel();
                        supect.setId(rs.getLong("ID"));
                        supect.setType(rs.getInt("TYPE"));
                        supect.setBus_id(rs.getInt("BUSINESSID"));
                        supect.setData_type(rs.getInt("DATATYPE"));
                        supect.setData_value(rs.getString("DATAVALUE"));
                        supects1.add(supect);
                    }
                } finally {
                    rs.close();
                }
            } finally {
                ps.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBFactory.close(rs, ps, conn);
        }
        last_fetch_time_supect = System.currentTimeMillis();
        supects=supects1;
        return supects1;
    }


    /**
     * 同步预警规则
     *
     * @return
     */
    public List<WarningRuleModel> getRules() {

        if (rules != null) {
            if (System.currentTimeMillis() - last_fetch_time < sync_period) {
                return rules;
            }
        }

        ArrayList<WarningRuleModel> rules1 = new ArrayList<WarningRuleModel>();

        Connection conn = null;
        try {
            conn = DBFactory.getInstance().getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (conn == null) {
            return rules;
        }
        String sql = "select * from WJ_EARYLWARNING_RULE where STATUS=1";
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(sql);
            try {
                rs = ps.executeQuery();
                try {
                    while (rs.next()) {

                        WarningRuleModel rule = new WarningRuleModel();
                        rule.setAreas(rs.getString("AREAS"));
                        rule.setBus_id(rs.getInt("BUSINESSID"));
                        rule.setBus_type(rs.getInt("BUINESSTYPE"));
                        rule.setCreate_time(rs.getDate("CREATEDATE"));
                        rule.setCreater(rs.getInt("CREATER"));
                        rule.setStatus(rs.getInt("STATUS"));
                        rule.setWarning(rs.getString("EARYWARNING"));
                        rule.setSupecters(rs.getString("SUPECTERS"));
                        rule.setRule_vlaue(rs.getString("RULEVALUE"));
                        rule.setRule_type(rs.getInt("RULETYPE"));
                        rule.setType(rs.getInt("TYPE"));
                        rule.setId(rs.getLong("ID"));
                        rule.setRule_name(rs.getString("RULENAME"));
                        rule.setPhone_number(rs.getString("phonenumber"));
                        rules1.add(rule);
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
        last_fetch_time = System.currentTimeMillis();
        rules=rules1;
        return rules;
    }
    public List<YxwjModel> getYxwjRules() {

        if (yxwjRules != null) {
            if (System.currentTimeMillis() - last_fetch_time < sync_period) {
                return yxwjRules;
            }
        }

        ArrayList<YxwjModel> yxwjRules1 = new ArrayList<YxwjModel>();

        Connection conn = null;
        try {
            conn = DBFactory.getInstance().getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (conn == null) {
            return yxwjRules;
        }
        String sql = "select * from WJ_YXWJ where SFSX=1";
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(sql);
            try {
                rs = ps.executeQuery();
                try {
                    while (rs.next()) {
                        YxwjModel yxwj=new YxwjModel();
                        yxwj.setID(rs.getLong("ID"));
                        yxwj.setGXSJ(rs.getDate("GXSJ"));
                        yxwj.setINPUTERINPUTER(rs.getString("INPUTER"));
                        yxwj.setJSONPARAM(rs.getString("JSONPARAM"));
                        yxwj.setNRMB(rs.getString("NRMB"));
                        yxwj.setQYID(rs.getString("QYID"));
                        yxwj.setSFSX(rs.getInt("SFSX"));
                        yxwj.setSXSJ(rs.getDate("SXSJ"));
                        yxwj.setYCLX(rs.getString("YCLX"));
                        yxwj.setYCSM(rs.getString("YCSM"));
                        yxwj.setYHID(rs.getString("YHID"));
                        yxwjRules1.add(yxwj);
                    }
                } catch (Exception e){
                    e.printStackTrace();
                }
                finally {
                    rs.close();
                }
            } finally {
                ps.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBFactory.close(rs, ps, conn);
        }
        last_fetch_time = System.currentTimeMillis();
        yxwjRules=yxwjRules1;
        return yxwjRules;
    }

    public Map<String, WbModel> getWbInfos() {
        if (wbInfos != null) {
            if (System.currentTimeMillis() - last_fetch_time < sync_period) {
                return wbInfos;
            }
        }

        HashMap<String, WbModel> wbInfos1 = new HashMap<String, WbModel>();

        Connection conn = null;
        try {
            conn = DBFactory.getInstance().getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (conn == null) {
            return wbInfos;
        }
        String sql = "select ID,LONGITUDE,LATITUDE from WJ_LOCATION_INFO where STATUS=1";
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(sql);
            try {
                rs = ps.executeQuery();
                try {
                    while (rs.next()) {

                        WbModel wbInfo = new WbModel();
                        wbInfo.setCode(rs.getString("ID"));
                        wbInfo.setLatitude(rs.getString("LATITUDE"));
                        wbInfo.setLongitude(rs.getString("LONGITUDE"));
                        wbInfos1.put(wbInfo.getCode(), wbInfo);
                    }
                } finally {
                    rs.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                ps.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBFactory.close(rs, ps, conn);
        }
        last_fetch_time = System.currentTimeMillis();
        wbInfos=wbInfos1;
        return wbInfos;
    }

    //
    public static void main(String args[]) {
//
//        TrackPoint tp = new TrackPoint();
//        tp.setName("test");
//        tp.setLat(1.f);
//        tp.setLng(2.f);
//        tp.setJgsj(new java.util.Date());
//        DataService.getInstance().writeResult2DB(tp);

//        List<WarningRuleModel> rules = RuleService.getInstance().getRules();
//        System.out.println(rules.size());
//        for(WarningRuleModel wrm:rules)
//            System.out.println(wrm.toString());

        List<SupectModel> supects = RuleService.getInstance().getSupectsByIds(new String[]{"181105596901668","2"});
        for(SupectModel supect:supects){
            System.out.println(supect.toString());
        }

/*        List<KeyAreaModel> areas = RuleService.getInstance().getAreasByIds(new String[]{"1", "2"});
        for (KeyAreaModel area : areas)
            System.out.println(area);*/
    }


}
