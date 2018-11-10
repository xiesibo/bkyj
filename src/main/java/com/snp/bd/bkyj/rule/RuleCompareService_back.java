package com.snp.bd.bkyj.rule;

import com.google.common.base.Strings;
import com.snp.bd.bkyj.model.*;
import com.snp.bd.bkyj.msg.AbstractMessage;
import com.snp.bd.bkyj.msg.DXMessage;
import com.snp.bd.bkyj.msg.HDMessage;
import com.snp.bd.bkyj.msg.WBMessage;
import com.snp.bd.bkyj.redis.RedisService;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created by Administrator on 2017/11/28.
 */
public class RuleCompareService_back implements Serializable {

/*    private static Logger logger = Logger.getLogger("比对服务");
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    *//**
     * 每条消息 比对规则
     *
     * @param message
     *//*
    public static List<WarningModel> compare(AbstractMessage message) {

        //比对所有规则
        List<WarningRuleModel> rules = new ArrayList<WarningRuleModel>();
        List<WarningRuleModel> tmps = RuleService.getInstance().getRules();
        rules.addAll(tmps);

        //挨个规则查看
        for (WarningRuleModel r : rules) {

//            logger.warn("当前规则:"+r.getRule_name());
            //获取线索ID列表
            String sp = r.getSupecters();
            if (sp == null) {
                logger.warn("规则[" + r.getId() + "]中线索为null");
                continue;
            }
            String ids[] = sp.split(",");

            //获取区域ID列表
            String ar = r.getAreas();
            if (ar == null) {
                logger.warn("规则[" + r.getId() + "]中区域信息为null");
                continue;
            }
            String[] ids_area = ar.split(",");

            //获取所有线索
            List<SupectModel> supects = RuleService.getInstance().getSupectsByIds(ids);
            List<KeyAreaModel> areas = RuleService.getInstance().getAreasByIds(ids_area);

            //规则类型
            switch (r.getType()) {

                case WarningRuleModel.TYPE_ENTER:  //进入
                    return execute(message, supects, areas, r);
                case WarningRuleModel.TYPE_LEAVE:  //离开
                    return execute(message, supects, areas, r);
                case WarningRuleModel.TYPE_DENSITY: //密度
                    return executeMd(message, supects, areas, r);
                case WarningRuleModel.TYPE_JOIN:  //聚集
                    return executeJj(message, supects, areas, r);
                case WarningRuleModel.TYPE_MISS:  //失联
                    break;

            }
        }
        return null;
    }


    *//**
     * 进入离开预警检测
     *
     * @param message xiaoxi
     * @param supects 线索
     * @param areas   区域
     * @param rule    规则
     *//*
    private static List<WarningModel> execute(AbstractMessage message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule) {
        Map<String,WbModel> wbInfos=RuleService.getInstance().getWbInfos();
        //产生的告警消息列表
        List<WarningModel> warns = new ArrayList<WarningModel>();
        //挨个分析线索
        for (SupectModel sm : supects) {
            //线索对应的案件信息
            CaseInfoModel cim = RuleService.getInstance().getCaseInfoById(sm.getBus_id());
            String users = cim.getUser_ids();
            String[] uids = users.split(",");
            switch (sm.getData_type()) {
                case SupectModel.DATA_TYPE_PHONE:    //电话号码
                    //话单数据中有电话号码
                    if (message instanceof HDMessage) {
                        HDMessage hd = (HDMessage) message;
                        if (sm.getData_value().equals(hd.getUsernum())) {
                            logger.info("话单记录-手机号码相同:" + hd.getUsernum());


                            //获取之前的状态
                            StateModel state = RedisService.getInstance().getState(hd.getUsernum());
//                            if(state != null)
//                                logger.info("Last state model:"+ JSONObject.toJSONString(state));

                            for (KeyAreaModel area : areas) {
                                int last_r = 0;
                                if (state != null)
                                    last_r = inOrOut(area, state.getLat(), state.getLng());
                                int r = inOrOut(area, Float.parseFloat(hd.getLatitude()), Float.parseFloat(hd.getLongitude()));
                                if (r == 1 && rule.getType() == WarningRuleModel.TYPE_ENTER && (last_r == -1 || last_r == 0)) { //进入
                                    for (String uid : uids) {
                                        if (Strings.isNullOrEmpty(uid)) continue;
                                        String msg = "话单-手机号:" + hd.getUsernum() + "进入区域:" + area.getName() + "[" + area.getId() + "]";
                                        WarningModel warn = generate(sm.getBus_id(), hd.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                        warns.add(warn);
                                    }
                                } else if (r == -1 && rule.getType() == WarningRuleModel.TYPE_LEAVE && (last_r == 0 || last_r == 1)) {  //离开
                                    for (String uid : uids) {
                                        if (Strings.isNullOrEmpty(uid)) continue;
                                        String msg = "话单-手机号:" + hd.getUsernum() + "离开区域:" + area.getName() + "[" + area.getId() + "]";
                                        WarningModel warn = generate(sm.getBus_id(), hd.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                        warns.add(warn);
                                    }
                                }
                            }

                            //保存最新状态  把手机号关联的信息状态都保存
                            StateModel s = new StateModel();
                            s.setLat(Float.parseFloat(hd.getLatitude()));
                            s.setLng(Float.parseFloat(hd.getLongitude()));
                            s.setDate(hd.getBegintime());
                            RedisService.getInstance().set(s, hd.getUsernum(), hd.getImei(), hd.getImsi());
                        }
                    } else if (message instanceof DXMessage) {
                        DXMessage dx = (DXMessage) message;
                        if (dx.getUsernum().equals(sm.getData_value())) {
                            logger.info("短信记录 - 手机号码相同:" + dx.getUsernum());

                            //获取之前的状态
                            StateModel state = RedisService.getInstance().getState(dx.getUsernum());

                            for (KeyAreaModel area : areas) {
                                int last_r = 0;
                                if (state != null)
                                    last_r = inOrOut(area, state.getLat(), state.getLng());

                                int r = inOrOut(area, Float.parseFloat(dx.getLatitude()), Float.parseFloat(dx.getLongitude()));
                                if (r == 1 && rule.getType() == WarningRuleModel.TYPE_ENTER && (last_r == 0 || last_r == -1)) { //进入
                                    for (String uid : uids) {
                                        if (Strings.isNullOrEmpty(uid)) continue;
                                        String msg = "短信-手机号:" + dx.getUsernum() + "进入区域:" + area.getName() + "[" + area.getId() + "]";
                                        WarningModel warn = generate(sm.getBus_id(), dx.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_DX, uid);
                                        warns.add(warn);
                                    }
                                }
                                if (r == -1 && rule.getType() == WarningRuleModel.TYPE_LEAVE && (last_r == 0 || last_r == 1)) { //离开
                                    for (String uid : uids) {
                                        if (Strings.isNullOrEmpty(uid)) continue;
                                        String msg = "短信-手机号:" + dx.getUsernum() + "离开区域:" + area.getName() + "[" + area.getId() + "]";
                                        WarningModel warn = generate(sm.getBus_id(), dx.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_DX, uid);
                                        warns.add(warn);
                                    }
                                }
                            }

                            //保存最新状态  把手机号关联的信息状态都保存
                            StateModel s = new StateModel();
                            s.setLat(Float.parseFloat(dx.getLatitude()));
                            s.setLng(Float.parseFloat(dx.getLongitude()));
                            s.setDate(dx.getBegintime());
                            RedisService.getInstance().set(s, dx.getUsernum(), dx.getImei(), dx.getImsi());
                        }
                    }
                    break;
                case SupectModel.DATA_TYPE_IMEI:  //IMEI
                    //话单数据中有IMEI号码
                    if (message instanceof HDMessage) {
                        HDMessage hd = (HDMessage) message;
                        if (sm.getData_value().equals(hd.getImei())) {
                            logger.info("话单记录 - IMEI号码相同:" + hd.getImei());

                            //获取之前的状态
                            StateModel state = RedisService.getInstance().getState(hd.getImei());

                            for (KeyAreaModel area : areas) {

                                int last_r = 0;
                                if (state != null)
                                    last_r = inOrOut(area, state.getLat(), state.getLng());

                                int r = inOrOut(area, Float.parseFloat(hd.getLatitude()), Float.parseFloat(hd.getLongitude()));
                                if (r == 1 && rule.getType() == WarningRuleModel.TYPE_ENTER && (last_r == 0 || last_r == -1)) { //进入
                                    for (String uid : uids) {
                                        if (Strings.isNullOrEmpty(uid)) continue;
                                        String msg = "短信-IMEI号:" + hd.getImei() + "进入区域:" + area.getName() + "[" + area.getId() + "]";
                                        WarningModel warn = generate(sm.getBus_id(), hd.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                        warns.add(warn);
                                    }
                                }
                                if (r == -1 && rule.getType() == WarningRuleModel.TYPE_LEAVE && (last_r == 0 || last_r == 1)) { //离开
                                    for (String uid : uids) {
                                        if (Strings.isNullOrEmpty(uid)) continue;
                                        String msg = "短信-IMEI号:" + hd.getImei() + "离开区域:" + area.getName() + "[" + area.getId() + "]";
                                        WarningModel warn = generate(sm.getBus_id(), hd.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                        warns.add(warn);
                                    }
                                }

                            }
                            //保存最新状态  把手机号关联的信息状态都保存
                            StateModel s = new StateModel();
                            s.setLat(Float.parseFloat(hd.getLatitude()));
                            s.setLng(Float.parseFloat(hd.getLongitude()));
                            s.setDate(hd.getBegintime());
                            RedisService.getInstance().set(s, hd.getUsernum(), hd.getImei(), hd.getImsi());

                        }
                    } else if (message instanceof DXMessage) {   //短信数据中有IMEI号码
                        DXMessage dx = (DXMessage) message;
                        if (dx.getImei().equals(sm.getData_value())) {
                            logger.info("短信记录 - IMEI号码相同:" + dx.getImei());

                            //获取之前的状态
                            StateModel state = RedisService.getInstance().getState(dx.getImei());

                            for (KeyAreaModel area : areas) {
                                int last_r = 0;
                                if (state != null)
                                    last_r = inOrOut(area, state.getLat(), state.getLng());

                                int r = inOrOut(area, Float.parseFloat(dx.getLatitude()), Float.parseFloat(dx.getLongitude()));
                                if (r == 1 && rule.getType() == WarningRuleModel.TYPE_ENTER && (last_r == 0 || last_r == -1)) { //进入
                                    for (String uid : uids) {
                                        if (Strings.isNullOrEmpty(uid)) continue;
                                        String msg = "短信-IMEI号:" + dx.getImei() + "进入区域:" + area.getName() + "[" + area.getId() + "]";
                                        WarningModel warn = generate(sm.getBus_id(), dx.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_DX, uid);
                                        warns.add(warn);
                                    }
                                }
                                if (r == -1 && rule.getType() == WarningRuleModel.TYPE_LEAVE && (last_r == 0 || last_r == 1)) { //离开
                                    for (String uid : uids) {
                                        if (Strings.isNullOrEmpty(uid)) continue;
                                        String msg = "短信-IMEI号:" + dx.getImei() + "离开区域:" + area.getName() + "[" + area.getId() + "]";
                                        WarningModel warn = generate(sm.getBus_id(), dx.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_DX, uid);
                                        warns.add(warn);
                                    }
                                }
                            }

                            //保存最新状态  把手机号关联的信息状态都保存
                            StateModel s = new StateModel();
                            s.setLat(Float.parseFloat(dx.getLatitude()));
                            s.setLng(Float.parseFloat(dx.getLongitude()));
                            s.setDate(dx.getBegintime());
                            RedisService.getInstance().set(s, dx.getUsernum(), dx.getImei(), dx.getImsi());
                        }

                    }
                    break;
                case SupectModel.DATA_TYPE_IMSI: //IMSI
                    //话单数据中有IMSI号码
                    if (message instanceof HDMessage) {
                        HDMessage hd = (HDMessage) message;
                        if (sm.getData_value().equals(hd.getImsi())) {
                            logger.info("话单记录 - IMSI号码相同:" + hd.getImsi());

                            //获取之前的状态
                            StateModel state = RedisService.getInstance().getState(hd.getImsi());

                            for (KeyAreaModel area : areas) {

                                int last_r = 0;
                                if (state != null)
                                    last_r = inOrOut(area, state.getLat(), state.getLng());

                                int r = inOrOut(area, Float.parseFloat(hd.getLatitude()), Float.parseFloat(hd.getLongitude()));
                                if (r == 1 && rule.getType() == WarningRuleModel.TYPE_ENTER && (last_r == 0 || last_r == -1)) { //进入
                                    for (String uid : uids) {
                                        if (Strings.isNullOrEmpty(uid)) continue;
                                        String msg = "短信-IMSI号:" + hd.getImsi() + "进入区域:" + area.getName() + "[" + area.getId() + "]";
                                        WarningModel warn = generate(sm.getBus_id(), hd.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                        warns.add(warn);
                                    }
                                }
                                if (r == -1 && rule.getType() == WarningRuleModel.TYPE_LEAVE && (last_r == 0 || last_r == 1)) { //离开
                                    for (String uid : uids) {
                                        if (Strings.isNullOrEmpty(uid)) continue;
                                        String msg = "短信-IMSI号:" + hd.getImsi() + "离开区域:" + area.getName() + "[" + area.getId() + "]";
                                        WarningModel warn = generate(sm.getBus_id(), hd.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                        warns.add(warn);
                                    }
                                }

                            }
                            //保存最新状态  把手机号关联的信息状态都保存
                            StateModel s = new StateModel();
                            s.setLat(Float.parseFloat(hd.getLatitude()));
                            s.setLng(Float.parseFloat(hd.getLongitude()));
                            s.setDate(hd.getBegintime());
                            RedisService.getInstance().set(s, hd.getUsernum(), hd.getImei(), hd.getImsi());

                        }
                    } else if (message instanceof DXMessage) {   //短信数据中有IMSI号码
                        DXMessage dx = (DXMessage) message;
                        if (dx.getImsi().equals(sm.getData_value())) {
                            logger.info("短信记录 - IMSI号码相同:" + dx.getImsi());
                            //获取之前的状态
                            StateModel state = RedisService.getInstance().getState(dx.getImsi());
                            for (KeyAreaModel area : areas) {
                                int last_r = 0;
                                if (state != null)
                                    last_r = inOrOut(area, state.getLat(), state.getLng());

                                int r = inOrOut(area, Float.parseFloat(dx.getLatitude()), Float.parseFloat(dx.getLongitude()));
                                if (r == 1 && rule.getType() == WarningRuleModel.TYPE_ENTER && (last_r == 0 || last_r == -1)) { //进入
                                    for (String uid : uids) {
                                        if (Strings.isNullOrEmpty(uid)) continue;
                                        String msg = "短信-IMSI号:" + dx.getImsi() + "进入区域:" + area.getName() + "[" + area.getId() + "]";
                                        WarningModel warn = generate(sm.getBus_id(), dx.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_DX, uid);
                                        warns.add(warn);
                                    }
                                }
                                if (r == -1 && rule.getType() == WarningRuleModel.TYPE_LEAVE && (last_r == 0 || last_r == 1)) { //离开
                                    for (String uid : uids) {
                                        if (Strings.isNullOrEmpty(uid)) continue;
                                        String msg = "短信-IMSI号:" + dx.getImsi() + "离开区域:" + area.getName() + "[" + area.getId() + "]";
                                        WarningModel warn = generate(sm.getBus_id(), dx.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_DX, uid);
                                        warns.add(warn);
                                    }
                                }
                            }
                            //保存最新状态  把手机号关联的信息状态都保存
                            StateModel s = new StateModel();
                            s.setLat(Float.parseFloat(dx.getLatitude()));
                            s.setLng(Float.parseFloat(dx.getLongitude()));
                            s.setDate(dx.getBegintime());
                            RedisService.getInstance().set(s, dx.getUsernum(), dx.getImei(), dx.getImsi());
                        }
                    }
                    break;
                case SupectModel.DATA_TYPE_CARD: //身份证号
                    if (message instanceof WBMessage) {
                        WBMessage wb = (WBMessage) message;
                        if (sm.getData_value().equals(wb.getId_card())) {
                            //获取之前的状态
                            StateModel state = RedisService.getInstance().getState(wb.getId_card());
                            for (KeyAreaModel area : areas) {
                                int last_r = 0;
                                if (state != null){
                                    last_r = inOrOut(area, state.getLat(), state.getLng());}
                                WbModel wbm= wbInfos.get(wb.getNet_code());
                                int r = inOrOut(area, Float.parseFloat(wbm.getLatitude()), Float.parseFloat(wbm.getLongitude()));
                                if (r == 1 && rule.getType() == WarningRuleModel.TYPE_ENTER && (last_r == -1 || last_r == 0)) { //进入
                                    for (String uid : uids) {
                                        if (Strings.isNullOrEmpty(uid)) continue;
                                        String msg = "网吧-身份证:" + wb.getId_card() + "进入区域:" + area.getName() + "[" + area.getId() + "]";
                                        WarningModel warn = generate(sm.getBus_id(), wb.getUp_time().toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                        warns.add(warn);
                                    }
                                } else if (r == -1 && rule.getType() == WarningRuleModel.TYPE_LEAVE && (last_r == 0 || last_r == 1)) {  //离开
                                    for (String uid : uids) {
                                        if (Strings.isNullOrEmpty(uid)) continue;
                                        String msg = "网吧-身份证:" + wb.getId_card() + "离开区域:" + area.getName() + "[" + area.getId() + "]";
                                        WarningModel warn = generate(sm.getBus_id(),wb.getUp_time().toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                        warns.add(warn);
                                    }
                                }
                            }
                        }
                    }
                    break;
                case SupectModel.DATA_TYPE_MAC: //MAC地址
                    break;
                case SupectModel.DATA_TYPE_QQ: //QQ号码
                    break;
                case SupectModel.DATA_TYPE_WECHART: //微信
                    break;
            }
        }
        return warns;

    }
    private static List<WarningModel> executeMd(AbstractMessage message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule) {
        Map<String,WbModel> wbInfos=RuleService.getInstance().getWbInfos();
        //产生的告警消息列表
        List<WarningModel> warns = new ArrayList<WarningModel>();
        if (message instanceof HDMessage) {
            HDMessage hd = (HDMessage) message;
            //获取之前的状态
            StateModel state = RedisService.getInstance().getState(hd.getUsernum());
            for (KeyAreaModel area : areas) {
                int last_r = 0;
                if (state != null){
                    last_r = inOrOut(area, state.getLat(), state.getLng());}
                int r = inOrOut(area, Float.parseFloat(hd.getLatitude()), Float.parseFloat(hd.getLongitude()));
                if (r == 1  && (last_r == -1 || last_r == 0)) { //进入
                    //TODO 判断区域中是否已经有这个人（身份证，手机号。。。对应同一个人）

                    //TODO 保存进入区域信息的时候根据已知信息补全信息
                } else if (r == -1 && (last_r == 0 || last_r == 1)) {  //离开

                }
            }
        }
        return warns;

    }

    *//**
     * 聚集
     * @param message
     * @param supects
     * @param areas
     * @param rule
     * @return
     *//*
    private static List<WarningModel> executeJj(AbstractMessage message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule){
        Map<String,WbModel> wbInfos=RuleService.getInstance().getWbInfos();
        //产生的告警消息列表
        List<WarningModel> warns = new ArrayList<WarningModel>();
        //挨个分析线索
        for (SupectModel sm : supects) {
            //线索对应的案件信息
            CaseInfoModel cim = RuleService.getInstance().getCaseInfoById(sm.getBus_id());
            String users = cim.getUser_ids();
            String[] uids = users.split(",");
            switch (sm.getData_type()) {
                case SupectModel.DATA_TYPE_PHONE:    //电话号码
                    //话单数据中有电话号码
                    if (message instanceof HDMessage) {
                        HDMessage hd = (HDMessage) message;
                        if (sm.getData_value().equals(hd.getUsernum())) {
                            logger.info("话单记录-手机号码相同:" + hd.getUsernum());
                            //获取之前的状态
                            StateModel state = RedisService.getInstance().getState(hd.getUsernum());
                            for (KeyAreaModel area : areas) {
                                int last_r = 0;
                                if (state != null)
                                    last_r = inOrOut(area, state.getLat(), state.getLng());
                                int r = inOrOut(area, Float.parseFloat(hd.getLatitude()), Float.parseFloat(hd.getLongitude()));
                                if (r == 1  && (last_r == -1 || last_r == 0)) { //进入
                                    //获取规则区域人数
                                    String renshu=RedisService.getInstance().get(area.getId()+String.valueOf(rule.getId()));
                                    if(renshu!=null){
                                        if(rule.getRule_type()==1){
                                            //所有人到指定区域才触发警报
                                            if((Long.valueOf(renshu)+1)==supects.size()){
                                                String msg = "总共" + Long.valueOf(renshu)+1 + "人进入区域:" + area.getName() + "[" + area.getId() + "]";
                                                for (String uid : uids) {
                                                    if (Strings.isNullOrEmpty(uid)) continue;
                                                    WarningModel warn = generate(sm.getBus_id(), hd.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                    warns.add(warn);
                                                }
                                            }
                                        }else {
                                            //到指定比例触发警报
                                            if((Long.valueOf(renshu)+1)==supects.size()*Double.valueOf(rule.getRule_vlaue())){
                                                String msg = "总共" + Long.valueOf(renshu)+1 + "人进入区域:" + area.getName() +"，比例为："+Double.valueOf(rule.getRule_vlaue())*100+ "[" + area.getId() + "]";
                                                for (String uid : uids) {
                                                    if (Strings.isNullOrEmpty(uid)) continue;
                                                    WarningModel warn = generate(sm.getBus_id(), hd.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                    warns.add(warn);
                                                }
                                            }
                                        }
                                    }
                                    RedisService.getInstance().set(area.getId()+String.valueOf(rule.getId()),String.valueOf(Integer.valueOf(renshu)+1));
                                } else if (r == -1 && (last_r == 0 || last_r == 1)) {  //离开
                                    String renshu=RedisService.getInstance().get(area.getId()+String.valueOf(rule.getId()));
                                    if(renshu!=null){
                                        RedisService.getInstance().set(area.getId()+String.valueOf(rule.getId()),String.valueOf(Integer.valueOf(renshu)-1));
                                    }
                                }
                            }

                            //保存最新状态  把手机号关联的信息状态都保存
                            StateModel s = new StateModel();
                            s.setLat(Float.parseFloat(hd.getLatitude()));
                            s.setLng(Float.parseFloat(hd.getLongitude()));
                            s.setDate(hd.getBegintime());
                            RedisService.getInstance().set(s, hd.getUsernum(), hd.getImei(), hd.getImsi());
                        }
                    } else if (message instanceof DXMessage) {
                        DXMessage dx = (DXMessage) message;
                        if (dx.getUsernum().equals(sm.getData_value())) {
                            logger.info("短信记录 - 手机号码相同:" + dx.getUsernum());
                            //获取之前的状态
                            StateModel state = RedisService.getInstance().getState(dx.getUsernum());

                            for (KeyAreaModel area : areas) {
                                int last_r = 0;
                                if (state != null)
                                    last_r = inOrOut(area, state.getLat(), state.getLng());

                                int r = inOrOut(area, Float.parseFloat(dx.getLatitude()), Float.parseFloat(dx.getLongitude()));
                                if (r == 1  && (last_r == 0 || last_r == -1)) { //进入
                                    //获取规则区域人数
                                    String renshu=RedisService.getInstance().get(area.getId()+String.valueOf(rule.getId()));
                                    if(renshu!=null){
                                        if(rule.getRule_type()==1){
                                            //所有人到指定区域才触发警报
                                            if((Long.valueOf(renshu)+1)==supects.size()){
                                                String msg = "总共" + Long.valueOf(renshu)+1 + "人进入区域:" + area.getName() + "[" + area.getId() + "]";
                                                for (String uid : uids) {
                                                    if (Strings.isNullOrEmpty(uid)) continue;
                                                    WarningModel warn = generate(sm.getBus_id(), dx.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                    warns.add(warn);
                                                }
                                            }
                                        }else {
                                            //到指定比例触发警报
                                            if((Long.valueOf(renshu)+1)==supects.size()*Double.valueOf(rule.getRule_vlaue())){
                                                String msg = "总共" + Long.valueOf(renshu)+1 + "人进入区域:" + area.getName() +"，比例为："+Double.valueOf(rule.getRule_vlaue())*100+ "[" + area.getId() + "]";
                                                for (String uid : uids) {
                                                    if (Strings.isNullOrEmpty(uid)) continue;
                                                    WarningModel warn = generate(sm.getBus_id(), dx.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                    warns.add(warn);
                                                }
                                            }
                                        }
                                    }
                                    RedisService.getInstance().set(area.getId()+String.valueOf(rule.getId()),String.valueOf(Integer.valueOf(renshu)+1));
                                }
                                if (r == -1  && (last_r == 0 || last_r == 1)) { //离开
                                    String renshu=RedisService.getInstance().get(area.getId()+String.valueOf(rule.getId()));
                                    if(renshu!=null){
                                        RedisService.getInstance().set(area.getId()+String.valueOf(rule.getId()),String.valueOf(Integer.valueOf(renshu)-1));
                                    }
                                }
                            }

                            //保存最新状态  把手机号关联的信息状态都保存
                            StateModel s = new StateModel();
                            s.setLat(Float.parseFloat(dx.getLatitude()));
                            s.setLng(Float.parseFloat(dx.getLongitude()));
                            s.setDate(dx.getBegintime());
                            RedisService.getInstance().set(s, dx.getUsernum(), dx.getImei(), dx.getImsi());
                        }
                    }
                    break;
                case SupectModel.DATA_TYPE_IMEI:  //IMEI
                    //话单数据中有IMEI号码
                    if (message instanceof HDMessage) {
                        HDMessage hd = (HDMessage) message;
                        if (sm.getData_value().equals(hd.getImei())) {
                            logger.info("话单记录 - IMEI号码相同:" + hd.getImei());

                            //获取之前的状态
                            StateModel state = RedisService.getInstance().getState(hd.getImei());

                            for (KeyAreaModel area : areas) {

                                int last_r = 0;
                                if (state != null)
                                    last_r = inOrOut(area, state.getLat(), state.getLng());

                                int r = inOrOut(area, Float.parseFloat(hd.getLatitude()), Float.parseFloat(hd.getLongitude()));
                                if (r == 1  && (last_r == 0 || last_r == -1)) { //进入
                                    //获取规则区域人数
                                    String renshu=RedisService.getInstance().get(area.getId()+String.valueOf(rule.getId()));
                                    if(renshu!=null){
                                        if(rule.getRule_type()==1){
                                            //所有人到指定区域才触发警报
                                            if((Long.valueOf(renshu)+1)==supects.size()){
                                                String msg = "总共" + Long.valueOf(renshu)+1 + "人进入区域:" + area.getName() + "[" + area.getId() + "]";
                                                for (String uid : uids) {
                                                    if (Strings.isNullOrEmpty(uid)) continue;
                                                    WarningModel warn = generate(sm.getBus_id(), hd.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                    warns.add(warn);
                                                }
                                            }
                                        }else {
                                            //到指定比例触发警报
                                            if((Long.valueOf(renshu)+1)==supects.size()*Double.valueOf(rule.getRule_vlaue())){
                                                String msg = "总共" + Long.valueOf(renshu)+1 + "人进入区域:" + area.getName() +"，比例为："+Double.valueOf(rule.getRule_vlaue())*100+ "[" + area.getId() + "]";
                                                for (String uid : uids) {
                                                    if (Strings.isNullOrEmpty(uid)) continue;
                                                    WarningModel warn = generate(sm.getBus_id(), hd.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                    warns.add(warn);
                                                }
                                            }
                                        }
                                    }
                                    RedisService.getInstance().set(area.getId()+String.valueOf(rule.getId()),String.valueOf(Integer.valueOf(renshu)+1));
                                }
                                if (r == -1  && (last_r == 0 || last_r == 1)) { //离开
                                    String renshu=RedisService.getInstance().get(area.getId()+String.valueOf(rule.getId()));
                                    if(renshu!=null){
                                        RedisService.getInstance().set(area.getId()+String.valueOf(rule.getId()),String.valueOf(Integer.valueOf(renshu)-1));
                                    }
                                }

                            }
                            //保存最新状态  把手机号关联的信息状态都保存
                            StateModel s = new StateModel();
                            s.setLat(Float.parseFloat(hd.getLatitude()));
                            s.setLng(Float.parseFloat(hd.getLongitude()));
                            s.setDate(hd.getBegintime());
                            RedisService.getInstance().set(s, hd.getUsernum(), hd.getImei(), hd.getImsi());

                        }
                    } else if (message instanceof DXMessage) {   //短信数据中有IMEI号码
                        DXMessage dx = (DXMessage) message;
                        if (dx.getImei().equals(sm.getData_value())) {
                            logger.info("短信记录 - IMEI号码相同:" + dx.getImei());

                            //获取之前的状态
                            StateModel state = RedisService.getInstance().getState(dx.getImei());

                            for (KeyAreaModel area : areas) {
                                int last_r = 0;
                                if (state != null)
                                    last_r = inOrOut(area, state.getLat(), state.getLng());

                                int r = inOrOut(area, Float.parseFloat(dx.getLatitude()), Float.parseFloat(dx.getLongitude()));
                                if (r == 1  && (last_r == 0 || last_r == -1)) { //进入
                                    //获取规则区域人数
                                    String renshu=RedisService.getInstance().get(area.getId()+String.valueOf(rule.getId()));
                                    if(renshu!=null){
                                        if(rule.getRule_type()==1){
                                            //所有人到指定区域才触发警报
                                            if((Long.valueOf(renshu)+1)==supects.size()){
                                                String msg = "总共" + Long.valueOf(renshu)+1 + "人进入区域:" + area.getName() + "[" + area.getId() + "]";
                                                for (String uid : uids) {
                                                    if (Strings.isNullOrEmpty(uid)) continue;
                                                    WarningModel warn = generate(sm.getBus_id(), dx.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                    warns.add(warn);
                                                }
                                            }
                                        }else {
                                            //到指定比例触发警报
                                            if((Long.valueOf(renshu)+1)==supects.size()*Double.valueOf(rule.getRule_vlaue())){
                                                String msg = "总共" + Long.valueOf(renshu)+1 + "人进入区域:" + area.getName() +"，比例为："+Double.valueOf(rule.getRule_vlaue())*100+ "[" + area.getId() + "]";
                                                for (String uid : uids) {
                                                    if (Strings.isNullOrEmpty(uid)) continue;
                                                    WarningModel warn = generate(sm.getBus_id(), dx.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                    warns.add(warn);
                                                }
                                            }
                                        }
                                    }
                                    RedisService.getInstance().set(area.getId()+String.valueOf(rule.getId()),String.valueOf(Integer.valueOf(renshu)+1));
                                }
                                if (r == -1  && (last_r == 0 || last_r == 1)) { //离开
                                    String renshu=RedisService.getInstance().get(area.getId()+String.valueOf(rule.getId()));
                                    if(renshu!=null){
                                        RedisService.getInstance().set(area.getId()+String.valueOf(rule.getId()),String.valueOf(Integer.valueOf(renshu)-1));
                                    }
                                }
                            }

                            //保存最新状态  把手机号关联的信息状态都保存
                            StateModel s = new StateModel();
                            s.setLat(Float.parseFloat(dx.getLatitude()));
                            s.setLng(Float.parseFloat(dx.getLongitude()));
                            s.setDate(dx.getBegintime());
                            RedisService.getInstance().set(s, dx.getUsernum(), dx.getImei(), dx.getImsi());
                        }

                    }
                    break;
                case SupectModel.DATA_TYPE_IMSI: //IMSI
                    //话单数据中有IMSI号码
                    if (message instanceof HDMessage) {
                        HDMessage hd = (HDMessage) message;
                        if (sm.getData_value().equals(hd.getImsi())) {
                            logger.info("话单记录 - IMSI号码相同:" + hd.getImsi());

                            //获取之前的状态
                            StateModel state = RedisService.getInstance().getState(hd.getImsi());

                            for (KeyAreaModel area : areas) {

                                int last_r = 0;
                                if (state != null)
                                    last_r = inOrOut(area, state.getLat(), state.getLng());

                                int r = inOrOut(area, Float.parseFloat(hd.getLatitude()), Float.parseFloat(hd.getLongitude()));
                                if (r == 1  && (last_r == 0 || last_r == -1)) { //进入
                                    //获取规则区域人数
                                    String renshu=RedisService.getInstance().get(area.getId()+String.valueOf(rule.getId()));
                                    if(renshu!=null){
                                        if(rule.getRule_type()==1){
                                            //所有人到指定区域才触发警报
                                            if((Long.valueOf(renshu)+1)==supects.size()){
                                                String msg = "总共" + Long.valueOf(renshu)+1 + "人进入区域:" + area.getName() + "[" + area.getId() + "]";
                                                for (String uid : uids) {
                                                    if (Strings.isNullOrEmpty(uid)) continue;
                                                    WarningModel warn = generate(sm.getBus_id(), hd.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                    warns.add(warn);
                                                }
                                            }
                                        }else {
                                            //到指定比例触发警报
                                            if((Long.valueOf(renshu)+1)==supects.size()*Double.valueOf(rule.getRule_vlaue())){
                                                String msg = "总共" + Long.valueOf(renshu)+1 + "人进入区域:" + area.getName() +"，比例为："+Double.valueOf(rule.getRule_vlaue())*100+ "[" + area.getId() + "]";
                                                for (String uid : uids) {
                                                    if (Strings.isNullOrEmpty(uid)) continue;
                                                    WarningModel warn = generate(sm.getBus_id(), hd.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                    warns.add(warn);
                                                }
                                            }
                                        }
                                    }
                                    RedisService.getInstance().set(area.getId()+String.valueOf(rule.getId()),String.valueOf(Integer.valueOf(renshu)+1));
                                }
                                if (r == -1  && (last_r == 0 || last_r == 1)) { //离开
                                    String renshu=RedisService.getInstance().get(area.getId()+String.valueOf(rule.getId()));
                                    if(renshu!=null){
                                        RedisService.getInstance().set(area.getId()+String.valueOf(rule.getId()),String.valueOf(Integer.valueOf(renshu)-1));
                                    }
                                }

                            }
                            //保存最新状态  把手机号关联的信息状态都保存
                            StateModel s = new StateModel();
                            s.setLat(Float.parseFloat(hd.getLatitude()));
                            s.setLng(Float.parseFloat(hd.getLongitude()));
                            s.setDate(hd.getBegintime());
                            RedisService.getInstance().set(s, hd.getUsernum(), hd.getImei(), hd.getImsi());

                        }
                    } else if (message instanceof DXMessage) {   //短信数据中有IMSI号码
                        DXMessage dx = (DXMessage) message;
                        if (dx.getImsi().equals(sm.getData_value())) {
                            logger.info("短信记录 - IMSI号码相同:" + dx.getImsi());
                            //获取之前的状态
                            StateModel state = RedisService.getInstance().getState(dx.getImsi());
                            for (KeyAreaModel area : areas) {
                                int last_r = 0;
                                if (state != null)
                                    last_r = inOrOut(area, state.getLat(), state.getLng());

                                int r = inOrOut(area, Float.parseFloat(dx.getLatitude()), Float.parseFloat(dx.getLongitude()));
                                if (r == 1  && (last_r == 0 || last_r == -1)) { //进入
                                    //获取规则区域人数
                                    String renshu=RedisService.getInstance().get(area.getId()+String.valueOf(rule.getId()));
                                    if(renshu!=null){
                                        if(rule.getRule_type()==1){
                                            //所有人到指定区域才触发警报
                                            if((Long.valueOf(renshu)+1)==supects.size()){
                                                String msg = "总共" + Long.valueOf(renshu)+1 + "人进入区域:" + area.getName() + "[" + area.getId() + "]";
                                                for (String uid : uids) {
                                                    if (Strings.isNullOrEmpty(uid)) continue;
                                                    WarningModel warn = generate(sm.getBus_id(), dx.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                    warns.add(warn);
                                                }
                                            }
                                        }else {
                                            //到指定比例触发警报
                                            if((Long.valueOf(renshu)+1)==supects.size()*Double.valueOf(rule.getRule_vlaue())){
                                                String msg = "总共" + Long.valueOf(renshu)+1 + "人进入区域:" + area.getName() +"，比例为："+Double.valueOf(rule.getRule_vlaue())*100+ "[" + area.getId() + "]";
                                                for (String uid : uids) {
                                                    if (Strings.isNullOrEmpty(uid)) continue;
                                                    WarningModel warn = generate(sm.getBus_id(), dx.getBegintime(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                    warns.add(warn);
                                                }
                                            }
                                        }
                                    }
                                    RedisService.getInstance().set(area.getId()+String.valueOf(rule.getId()),String.valueOf(Integer.valueOf(renshu)+1));
                                }
                                if (r == -1  && (last_r == 0 || last_r == 1)) { //离开
                                    String renshu=RedisService.getInstance().get(area.getId()+String.valueOf(rule.getId()));
                                    if(renshu!=null){
                                        RedisService.getInstance().set(area.getId()+String.valueOf(rule.getId()),String.valueOf(Integer.valueOf(renshu)-1));
                                    }
                                }
                            }
                            //保存最新状态  把手机号关联的信息状态都保存
                            StateModel s = new StateModel();
                            s.setLat(Float.parseFloat(dx.getLatitude()));
                            s.setLng(Float.parseFloat(dx.getLongitude()));
                            s.setDate(dx.getBegintime());
                            RedisService.getInstance().set(s, dx.getUsernum(), dx.getImei(), dx.getImsi());
                        }
                    }
                    break;
                case SupectModel.DATA_TYPE_CARD: //身份证号
                    if (message instanceof WBMessage) {
                        WBMessage wb = (WBMessage) message;
                        if (sm.getData_value().equals(wb.getId_card())) {
                            //获取之前的状态
                            StateModel state = RedisService.getInstance().getState(wb.getId_card());
                            for (KeyAreaModel area : areas) {
                                int last_r = 0;
                                if (state != null){
                                    last_r = inOrOut(area, state.getLat(), state.getLng());}
                                WbModel wbm= wbInfos.get(wb.getNet_code());
                                int r = inOrOut(area, Float.parseFloat(wbm.getLatitude()), Float.parseFloat(wbm.getLongitude()));
                                if (r == 1  && (last_r == -1 || last_r == 0)) { //进入
                                    //获取规则区域人数
                                    String renshu=RedisService.getInstance().get(area.getId()+String.valueOf(rule.getId()));
                                    if(renshu!=null){
                                        if(rule.getRule_type()==1){
                                            //所有人到指定区域才触发警报
                                            if((Long.valueOf(renshu)+1)==supects.size()){
                                                String msg = "总共" + Long.valueOf(renshu)+1 + "人进入区域:" + area.getName() + "[" + area.getId() + "]";
                                                for (String uid : uids) {
                                                    if (Strings.isNullOrEmpty(uid)) continue;
                                                    WarningModel warn = generate(sm.getBus_id(), wb.getUp_time().toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                    warns.add(warn);
                                                }
                                            }
                                        }else {
                                            //到指定比例触发警报
                                            if((Long.valueOf(renshu)+1)==supects.size()*Double.valueOf(rule.getRule_vlaue())){
                                                String msg = "总共" + Long.valueOf(renshu)+1 + "人进入区域:" + area.getName() +"，比例为："+Double.valueOf(rule.getRule_vlaue())*100+ "[" + area.getId() + "]";
                                                for (String uid : uids) {
                                                    if (Strings.isNullOrEmpty(uid)) continue;
                                                    WarningModel warn = generate(sm.getBus_id(), wb.getUp_time().toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                    warns.add(warn);
                                                }
                                            }
                                        }
                                    }
                                    RedisService.getInstance().set(area.getId()+String.valueOf(rule.getId()),String.valueOf(Integer.valueOf(renshu)+1));
                                } else if (r == -1  && (last_r == 0 || last_r == 1)) { //离开
                                    String renshu=RedisService.getInstance().get(area.getId()+String.valueOf(rule.getId()));
                                    if(renshu!=null){
                                        RedisService.getInstance().set(area.getId()+String.valueOf(rule.getId()),String.valueOf(Integer.valueOf(renshu)-1));
                                    }
                                }
                            }
                        }
                    }
                    break;
                case SupectModel.DATA_TYPE_MAC: //MAC地址
                    break;
                case SupectModel.DATA_TYPE_QQ: //QQ号码
                    break;
                case SupectModel.DATA_TYPE_WECHART: //微信
                    break;
            }
        }
        return warns;

    }
    *//**
     * 判断当前位置是否在区域内
     *
     * @param area 区域
     * @param lat  纬度
     * @param lng  经度
     * @return 1-进入了   -1-离开了 0-检测失败
     *//*
    private static int inOrOut(KeyAreaModel area, float lat, float lng) {
        Object o = area.convertArea();
        if (o != null) {
            if (o instanceof KeyAreaModel.Circular) {
                KeyAreaModel.Circular c = (KeyAreaModel.Circular) o;
                float x1 = lat;
                float y1 = lng;
                float r1 = (float) (Math.sqrt(Math.pow(x1 - c.getX(), 2) + Math.pow(y1 - c.getY(), 2)));
                if (r1 <= c.getR()) {   //说明当前位置在区域内
                    return 1;
                } else {
                    return -1;
                }
            } else {
                KeyAreaModel.Rectangle r = (KeyAreaModel.Rectangle) o;
                if (lng >= r.getXmin() && lng <= r.getXmax() && lat >= r.getYmin() && lat <= r.getYmax())
                    return 1;
                else
                    return -1;
            }

        } else {
            logger.warn("解析区域信息失败:" + area.getArea());
        }
        return 0;
    }

    *//**
     * @param bus_id      业务ID
     * @param begine_time 发生时间
     * @param msg
     * @param rule_id
     * @param type
     * @param uid
     * @return
     *//*
    private static WarningModel generate(int bus_id, String begine_time, String msg, int rule_id, int type, String uid) {
        WarningModel warn = new WarningModel();
        warn.setId(UUID.randomUUID().toString());
        warn.setBusinessid(bus_id);
        warn.setBuinesstype(1);  //默认1
        try {
            warn.setHappentime(sdf.parse(begine_time));
        } catch (Exception e) {
            logger.warn("日期转换异常:" + e.getMessage(), e);
        }
        warn.setIsready(2); //未读
        warn.setMsgcontent(msg);
        warn.setRuleid(rule_id);
        warn.setSourcetype(type);
        warn.setTotime(new Date());
        warn.setTouser(Integer.parseInt(uid));
        return warn;
    }*/


}
