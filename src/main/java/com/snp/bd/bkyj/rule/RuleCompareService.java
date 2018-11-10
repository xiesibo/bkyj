package com.snp.bd.bkyj.rule;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.snp.bd.bkyj.model.*;
import com.snp.bd.bkyj.msg.AbstractMessage;
import com.snp.bd.bkyj.msg.DXMessage;
import com.snp.bd.bkyj.msg.HDMessage;
import com.snp.bd.bkyj.msg.WBMessage;
import com.snp.bd.bkyj.redis.RedisService;
import com.snp.bd.bkyj.util.ExceptionCompareUtil;
import com.snp.bd.bkyj.util.HbaseService;
import com.snp.bd.bkyj.util.YxwjCompareUtil;
import org.apache.commons.digester.Rule;
import org.apache.log4j.Logger;

import javax.swing.plaf.nimbus.State;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created by Administrator on 2017/11/28.
 */
public class RuleCompareService implements Serializable {

    private static Logger logger = Logger.getLogger("比对服务");
    //    private static Map<String, String> tempCache;
//    private static List<WarningModel> warns;
    private static List<YxwjModel> yxwjRules;

    /**
     * 每条消息 比对规则
     *
     * @param message
     */
    public static List<WarningModel> compare(Msg message) {
        List<WarningModel> warns = new ArrayList<WarningModel>();
        //比对所有规则
        List<WarningRuleModel> rules = new ArrayList<WarningRuleModel>();
        List<WarningRuleModel> tmps = RuleService.getInstance().getRules();
        rules.addAll(tmps);
        Map<String, String> tempCache = new HashMap<>();
        yxwjRules = RuleService.getInstance().getYxwjRules();
        //对比所有隐性规则
        for (YxwjModel yxwjModel : yxwjRules) {
            //获取区域ID列表
            String ar = yxwjModel.getQYID();
            if (ar == null) {
                ar = "";
                logger.warn("规则[" + yxwjModel.getId() + "]中区域信息为null");
            }
            String[] ids_area = ar.split(",");
            List<KeyAreaModel> areas = null;
            if (ar.length() > 0)
                areas = RuleService.getInstance().getAreasByIds(ids_area);
            switch (yxwjModel.getYCLX()) {
                case YxwjModel.EX_TH_2:
                    //异常类型1（当天拨打电话超过2小时）
                    YxwjCompareUtil.executeExTh2(message, areas, yxwjModel, warns, tempCache);
                    break;
                case YxwjModel.EX_TH_0_4_1:
                    YxwjCompareUtil.executeEX_TH_0_4_1(message, areas, yxwjModel, warns, tempCache);
                    break;
                case YxwjModel.EX_TH_0_4_TD_1:
                    YxwjCompareUtil.executeEX_TH_0_4_TD_1(message, areas, yxwjModel, warns, tempCache);
                    break;
                case YxwjModel.EX_THCS_5:
                    YxwjCompareUtil.executeEX_THCS_5(message, areas, yxwjModel, warns, tempCache);
                    break;
                case YxwjModel.EX_TH_10:
                    YxwjCompareUtil.executeExTh10(message, areas, yxwjModel, warns, tempCache);
                    break;
                //当天凌晨0-4点通话超过10次
                case YxwjModel.EX_WB_15:
                    YxwjCompareUtil.executeEX_WB_15(message, areas, yxwjModel, warns, tempCache);
                    break;
                //当天凌晨0-4点拨打特定号码通话超过5次
                case YxwjModel.EX_TH_5:
                    YxwjCompareUtil.executeEX_TH_5(message, areas, yxwjModel, warns, tempCache);
                    break;
                //平均每月寄出快递数据超过15次
                case YxwjModel.EX_KD_15:
                    YxwjCompareUtil.executeEX_KD_15(message, areas, yxwjModel, warns, tempCache);
                    break;
                case YxwjModel.EX_KD_30:
                    YxwjCompareUtil.executeEX_KD_30(message, areas, yxwjModel, warns, tempCache);
                    break;
                case YxwjModel.EX_WB_20:
                    YxwjCompareUtil.executeEX_WB_20(message, areas, yxwjModel, warns, tempCache);
                    break;
                case YxwjModel.EX_TH_3FC:
                    YxwjCompareUtil.executeEX_TH_3FC(message, areas, yxwjModel, warns, tempCache);
                    break;
                case YxwjModel.EX_TH_0_4_3FC:
                    YxwjCompareUtil.executeEX_TH_0_4_3FC(message, areas, yxwjModel, warns, tempCache);
                    break;
                case YxwjModel.EX_TH_0_4_TD3FC:
                    YxwjCompareUtil.executeEX_TH_0_4_TD3FC(message, areas, yxwjModel, warns, tempCache);
                    break;
                case YxwjModel.EX_THCS_3FC:
                    YxwjCompareUtil.executeEX_THCS_3FC(message, areas, yxwjModel, warns, tempCache);
                    break;
                case YxwjModel.EX_THCS_0_4_3FC:
                    YxwjCompareUtil.executeEX_THCS_0_4_3FC(message, areas, yxwjModel, warns, tempCache);
                    break;
                case YxwjModel.EX_THCS_0_4_TD3FC:
                    YxwjCompareUtil.executeEX_THCS_0_4_TD3FC(message, areas, yxwjModel, warns, tempCache);
                    break;
                case YxwjModel.EX_KD_3FC:
                    YxwjCompareUtil.executeEX_KD_3FC(message, areas, yxwjModel, warns, tempCache);
                    break;
                case YxwjModel.EX_WB_3FC:
                    YxwjCompareUtil.executeEX_WB_3FC(message, areas, yxwjModel, warns, tempCache);
                    break;
            }
        }
        //挨个规则查看
        for (WarningRuleModel r : rules) {

//            logger.warn("当前规则:"+r.getRule_name());
            //获取线索ID列表
            String sp = r.getSupecters();
            if (sp == null) {
                sp = "";
                logger.warn("规则[" + r.getId() + "]中线索为null");
            }
            String ids[] = sp.split(",");

            //获取区域ID列表
            String ar = r.getAreas();
            if (ar == null) {
                ar = "";
                logger.warn("规则[" + r.getId() + "]中区域信息为null");
            }
            String[] ids_area = ar.split(",");

            //获取所有线索
            List<SupectModel> supects = null;
            if (sp.length() > 0)
                supects = RuleService.getInstance().getSupectsByIds(ids);
            List<KeyAreaModel> areas = null;
            if (ar.length() > 0)
                areas = RuleService.getInstance().getAreasByIds(ids_area);

            //规则类型
            switch (r.getType()) {

                case WarningRuleModel.TYPE_ENTER:  //进入
                    execute(message, supects, areas, r, warns, tempCache);
                    break;
                case WarningRuleModel.TYPE_LEAVE:  //离开
                    execute(message, supects, areas, r, warns, tempCache);
                    break;
                case WarningRuleModel.TYPE_DENSITY: //密度
                    executeMd(message, supects, areas, r, warns, tempCache);
                    break;
                case WarningRuleModel.TYPE_JOIN:  //聚集
                    executeJj(message, supects, areas, r, warns, tempCache);
                    break;
                case WarningRuleModel.TYPE_MISS:  //失联
                    executeSl(message, supects, areas, r, warns, tempCache);
                    break;
                case WarningRuleModel.TYPE_EX:  //异常
                    executeEx(message, supects, areas, r, warns, tempCache);
                    break;
            }
        }
        for (String key :
                tempCache.keySet()) {
            if (tempCache.get(key) != null) {
                RedisService.getInstance().set(key, tempCache.get(key));
            } else {
                logger.info("**** null:" + key);
            }
        }
        return warns;
    }


    /**
     * 进入离开预警检测
     *
     * @param message xiaoxi
     * @param supects 线索
     * @param areas   区域
     * @param rule    规则
     */
    private static List<WarningModel> execute(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        Map<String, WbModel> wbInfos = RuleService.getInstance().getWbInfos();
        //挨个分析线索
        for (SupectModel sm : supects) {
            logger.info(sm);
            //线索对应的案件信息
            CaseInfoModel cim = null;
            if (rule.getBus_type() == 1) {
                cim = RuleService.getInstance().getCaseInfoById(Integer.valueOf(rule.getBus_id()));
            }//是否为空
            if (cim != null && cim.getUser_ids() != null) {
                String users = cim.getUser_ids();
                String[] uids = users.split(",");
                switch (sm.getData_type()) {
                    case SupectModel.DATA_TYPE_PHONE:    //电话号码
                        //话单数据中有电话号码
                        if (message.getType() == Msg.wj_dw_cdr) {
                            Map hd = message.getContext();
                            if (sm.getData_value().equals(hd.get("phone"))) {
                                logger.info("话单记录-手机号码相同:" + hd.get("phone"));


                                //获取之前的状态
                                StateModel state = RedisService.getInstance().getState(hd.get("phone").toString() + "@@" + rule.getId());
//                            if(state != null)
//                                logger.info("Last state model:"+ JSONObject.toJSONString(state));

                                for (KeyAreaModel area : areas) {
                                    int last_r = 0;
                                    if (state != null)
                                        last_r = inOrOut(area, state.getLat(), state.getLng());
                                    int r = inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                                    if (r == 1 && rule.getType() == WarningRuleModel.TYPE_ENTER && (last_r == -1 || last_r == 0)) { //进入
                                        for (String uid : uids) {
                                            if (Strings.isNullOrEmpty(uid)) continue;
                                            String msg = "话单-手机号:" + hd.get("phone") + "进入区域:" + area.getName() + "[" + area.getId() + "]";
                                            WarningModel warn = generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                            warns.add(warn);
                                        }
                                    } else if (r == -1 && rule.getType() == WarningRuleModel.TYPE_LEAVE && (last_r == 0 || last_r == 1)) {  //离开
                                        for (String uid : uids) {
                                            if (Strings.isNullOrEmpty(uid)) continue;
                                            String msg = "话单-手机号:" + hd.get("phone") + "离开区域:" + area.getName() + "[" + area.getId() + "]";
                                            WarningModel warn = generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                            warns.add(warn);
                                        }
                                    }
                                }

                                //保存最新状态  把手机号关联的信息状态都保存
                                StateModel s = new StateModel();
                                s.setLat(Float.parseFloat(hd.get("lat").toString()));
                                s.setLng(Float.parseFloat(hd.get("lng").toString()));
                                s.setDate(hd.get("time").toString());
                                tempCache.put(hd.get("phone").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                tempCache.put(hd.get("imei").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                tempCache.put(hd.get("imsi").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));

                            }
                        } else if (message.getType() == Msg.wj_dw_sms) {
                            Map dx = message.getContext();
                            if (dx.get("phone").equals(sm.getData_value())) {
                                logger.info("短信记录 - 手机号码相同:" + dx.get("phone"));

                                //获取之前的状态
                                StateModel state = RedisService.getInstance().getState(dx.get("phone").toString() + "@@" + rule.getId());

                                for (KeyAreaModel area : areas) {
                                    int last_r = 0;
                                    if (state != null)
                                        last_r = inOrOut(area, state.getLat(), state.getLng());

                                    int r = inOrOut(area, Float.parseFloat(dx.get("lat").toString()), Float.parseFloat(dx.get("lng").toString()));
                                    if (r == 1 && rule.getType() == WarningRuleModel.TYPE_ENTER && (last_r == 0 || last_r == -1)) { //进入
                                        for (String uid : uids) {
                                            if (Strings.isNullOrEmpty(uid)) continue;
                                            String msg = "短信-手机号:" + dx.get("phone") + "进入区域:" + area.getName() + "[" + area.getId() + "]";
                                            WarningModel warn = generate(sm.getBus_id(), dx.get("sendtime").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_DX, uid);
                                            warns.add(warn);
                                        }
                                    }
                                    if (r == -1 && rule.getType() == WarningRuleModel.TYPE_LEAVE && (last_r == 0 || last_r == 1)) { //离开
                                        for (String uid : uids) {
                                            if (Strings.isNullOrEmpty(uid)) continue;
                                            String msg = "短信-手机号:" + dx.get("phone") + "离开区域:" + area.getName() + "[" + area.getId() + "]";
                                            WarningModel warn = generate(sm.getBus_id(), dx.get("sendtime").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_DX, uid);
                                            warns.add(warn);
                                        }
                                    }
                                }

                                //保存最新状态  把手机号关联的信息状态都保存
                                StateModel s = new StateModel();
                                s.setLat(Float.parseFloat(dx.get("lat").toString()));
                                s.setLng(Float.parseFloat(dx.get("lng").toString()));
                                s.setDate(dx.get("sendtime").toString());
                                tempCache.put(dx.get("phone").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                tempCache.put(dx.get("imei").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                tempCache.put(dx.get("imsi").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));

                            }
                        }
                        break;
                    case SupectModel.DATA_TYPE_IMEI:  //IMEI
                        //话单数据中有IMEI号码
                        if (message.getType() == Msg.wj_dw_cdr) {
                            Map hd = message.getContext();
                            if (sm.getData_value().equals(hd.get("imei").toString())) {
                                logger.info("话单记录 - IMEI号码相同:" + hd.get("imei").toString());

                                //获取之前的状态
                                StateModel state = RedisService.getInstance().getState(hd.get("imei").toString() + "@@" + rule.getId());

                                for (KeyAreaModel area : areas) {

                                    int last_r = 0;
                                    if (state != null)
                                        last_r = inOrOut(area, state.getLat(), state.getLng());

                                    int r = inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                                    if (r == 1 && rule.getType() == WarningRuleModel.TYPE_ENTER && (last_r == 0 || last_r == -1)) { //进入
                                        for (String uid : uids) {
                                            if (Strings.isNullOrEmpty(uid)) continue;
                                            String msg = "短信-IMEI号:" + hd.get("imei").toString() + "进入区域:" + area.getName() + "[" + area.getId() + "]";
                                            WarningModel warn = generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                            warns.add(warn);
                                        }
                                    }
                                    if (r == -1 && rule.getType() == WarningRuleModel.TYPE_LEAVE && (last_r == 0 || last_r == 1)) { //离开
                                        for (String uid : uids) {
                                            if (Strings.isNullOrEmpty(uid)) continue;
                                            String msg = "短信-IMEI号:" + hd.get("imei").toString() + "离开区域:" + area.getName() + "[" + area.getId() + "]";
                                            WarningModel warn = generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                            warns.add(warn);
                                        }
                                    }

                                }
                                //保存最新状态  把手机号关联的信息状态都保存
                                StateModel s = new StateModel();
                                s.setLat(Float.parseFloat(hd.get("lat").toString()));
                                s.setLng(Float.parseFloat(hd.get("lng").toString()));
                                s.setDate(hd.get("time").toString());
                                tempCache.put(hd.get("phone").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                tempCache.put(hd.get("imei").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                tempCache.put(hd.get("imsi").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                            }
                        } else if (message.getType() == Msg.wj_dw_sms) {   //短信数据中有IMEI号码
                            Map dx = message.getContext();
                            if (dx.get("imei").toString().equals(sm.getData_value())) {
                                logger.info("短信记录 - IMEI号码相同:" + dx.get("imei").toString());

                                //获取之前的状态
                                StateModel state = RedisService.getInstance().getState(dx.get("imei").toString() + "@@" + rule.getId());

                                for (KeyAreaModel area : areas) {
                                    int last_r = 0;
                                    if (state != null)
                                        last_r = inOrOut(area, state.getLat(), state.getLng());

                                    int r = inOrOut(area, Float.parseFloat(dx.get("lat").toString()), Float.parseFloat(dx.get("lng").toString()));
                                    if (r == 1 && rule.getType() == WarningRuleModel.TYPE_ENTER && (last_r == 0 || last_r == -1)) { //进入
                                        for (String uid : uids) {
                                            if (Strings.isNullOrEmpty(uid)) continue;
                                            String msg = "短信-IMEI号:" + dx.get("imei").toString() + "进入区域:" + area.getName() + "[" + area.getId() + "]";
                                            WarningModel warn = generate(sm.getBus_id(), dx.get("sendtime").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_DX, uid);
                                            warns.add(warn);
                                        }
                                    }
                                    if (r == -1 && rule.getType() == WarningRuleModel.TYPE_LEAVE && (last_r == 0 || last_r == 1)) { //离开
                                        for (String uid : uids) {
                                            if (Strings.isNullOrEmpty(uid)) continue;
                                            String msg = "短信-IMEI号:" + dx.get("imei").toString() + "离开区域:" + area.getName() + "[" + area.getId() + "]";
                                            WarningModel warn = generate(sm.getBus_id(), dx.get("sendtime").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_DX, uid);
                                            warns.add(warn);
                                        }
                                    }
                                }

                                //保存最新状态  把手机号关联的信息状态都保存
                                StateModel s = new StateModel();
                                s.setLat(Float.parseFloat(dx.get("lat").toString()));
                                s.setLng(Float.parseFloat(dx.get("lng").toString()));
                                s.setDate(dx.get("sendtime").toString());
                                tempCache.put(dx.get("phone").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                tempCache.put(dx.get("imei").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                tempCache.put(dx.get("imsi").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                            }

                        }
                        break;
                    case SupectModel.DATA_TYPE_IMSI: //IMSI
                        //话单数据中有IMSI号码
                        if (message.getType() == Msg.wj_dw_cdr) {
                            Map hd = message.getContext();
                            if (sm.getData_value().equals(hd.get("imsi").toString())) {
                                logger.info("话单记录 - IMSI号码相同:" + hd.get("imsi").toString());

                                //获取之前的状态
                                StateModel state = RedisService.getInstance().getState(hd.get("imsi").toString() + "@@" + rule.getId());

                                for (KeyAreaModel area : areas) {

                                    int last_r = 0;
                                    if (state != null)
                                        last_r = inOrOut(area, state.getLat(), state.getLng());

                                    int r = inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                                    if (r == 1 && rule.getType() == WarningRuleModel.TYPE_ENTER && (last_r == 0 || last_r == -1)) { //进入
                                        for (String uid : uids) {
                                            if (Strings.isNullOrEmpty(uid)) continue;
                                            String msg = "短信-IMSI号:" + hd.get("imsi").toString() + "进入区域:" + area.getName() + "[" + area.getId() + "]";
                                            WarningModel warn = generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                            warns.add(warn);
                                        }
                                    }
                                    if (r == -1 && rule.getType() == WarningRuleModel.TYPE_LEAVE && (last_r == 0 || last_r == 1)) { //离开
                                        for (String uid : uids) {
                                            if (Strings.isNullOrEmpty(uid)) continue;
                                            String msg = "短信-IMSI号:" + hd.get("imsi").toString() + "离开区域:" + area.getName() + "[" + area.getId() + "]";
                                            WarningModel warn = generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                            warns.add(warn);
                                        }
                                    }

                                }
                                //保存最新状态  把手机号关联的信息状态都保存
                                StateModel s = new StateModel();
                                s.setLat(Float.parseFloat(hd.get("lat").toString()));
                                s.setLng(Float.parseFloat(hd.get("lng").toString()));
                                s.setDate(hd.get("time").toString());
                                tempCache.put(hd.get("phone").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                tempCache.put(hd.get("imei").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                tempCache.put(hd.get("imsi").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                            }
                        } else if (message.getType() == Msg.wj_dw_sms) {   //短信数据中有IMSI号码
                            Map dx = message.getContext();
                            if (dx.get("imsi").toString().equals(sm.getData_value())) {
                                logger.info("短信记录 - IMSI号码相同:" + dx.get("imsi").toString());
                                //获取之前的状态
                                StateModel state = RedisService.getInstance().getState(dx.get("imsi").toString() + "@@" + rule.getId());
                                for (KeyAreaModel area : areas) {
                                    int last_r = 0;
                                    if (state != null)
                                        last_r = inOrOut(area, state.getLat(), state.getLng());

                                    int r = inOrOut(area, Float.parseFloat(dx.get("lat").toString()), Float.parseFloat(dx.get("lng").toString()));
                                    if (r == 1 && rule.getType() == WarningRuleModel.TYPE_ENTER && (last_r == 0 || last_r == -1)) { //进入
                                        for (String uid : uids) {
                                            if (Strings.isNullOrEmpty(uid)) continue;
                                            String msg = "短信-IMSI号:" + dx.get("imsi").toString() + "进入区域:" + area.getName() + "[" + area.getId() + "]";
                                            WarningModel warn = generate(sm.getBus_id(), dx.get("sendtime").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_DX, uid);
                                            warns.add(warn);
                                        }
                                    }
                                    if (r == -1 && rule.getType() == WarningRuleModel.TYPE_LEAVE && (last_r == 0 || last_r == 1)) { //离开
                                        for (String uid : uids) {
                                            if (Strings.isNullOrEmpty(uid)) continue;
                                            String msg = "短信-IMSI号:" + dx.get("imsi").toString() + "离开区域:" + area.getName() + "[" + area.getId() + "]";
                                            WarningModel warn = generate(sm.getBus_id(), dx.get("sendtime").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_DX, uid);
                                            warns.add(warn);
                                        }
                                    }
                                }
                                //保存最新状态  把手机号关联的信息状态都保存
                                StateModel s = new StateModel();
                                s.setLat(Float.parseFloat(dx.get("lat").toString()));
                                s.setLng(Float.parseFloat(dx.get("lng").toString()));
                                s.setDate(dx.get("sendtime").toString());
                                tempCache.put(dx.get("phone").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                tempCache.put(dx.get("imei").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                tempCache.put(dx.get("imsi").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                            }
                        }
                        break;
                    case SupectModel.DATA_TYPE_CARD: //身份证号
                        if (message.getType() == Msg.wj_net_use) {
                            Map wb = message.getContext();
                            if (sm.getData_value().equals(wb.get("id_card").toString())) {
                                //获取之前的状态
                                StateModel state = RedisService.getInstance().getState(wb.get("id_card").toString() + "@@" + rule.getId());
                                for (KeyAreaModel area : areas) {
                                    int last_r = 0;
                                    if (state != null) {
                                        last_r = inOrOut(area, state.getLat(), state.getLng());
                                    }
                                    WbModel wbm = wbInfos.get(wb.get("net_code").toString());
                                    int r = inOrOut(area, Float.parseFloat(wbm.getLatitude()), Float.parseFloat(wbm.getLongitude()));
                                    if (r == 1 && rule.getType() == WarningRuleModel.TYPE_ENTER && (last_r == -1 || last_r == 0)) { //进入
                                        for (String uid : uids) {
                                            if (Strings.isNullOrEmpty(uid)) continue;
                                            String msg = "网吧-身份证:" + wb.get("id_card").toString() + "进入区域:" + area.getName() + "[" + area.getId() + "]";
                                            WarningModel warn = generate(sm.getBus_id(), wb.get("up_time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                            warns.add(warn);
                                        }
                                    } else if (r == -1 && rule.getType() == WarningRuleModel.TYPE_LEAVE && (last_r == 0 || last_r == 1)) {  //离开
                                        for (String uid : uids) {
                                            if (Strings.isNullOrEmpty(uid)) continue;
                                            String msg = "网吧-身份证:" + wb.get("id_card").toString() + "离开区域:" + area.getName() + "[" + area.getId() + "]";
                                            WarningModel warn = generate(sm.getBus_id(), wb.get("up_time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
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
        }
        return warns;

    }

    private static List<WarningModel> executeMd(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        Map<String, WbModel> wbInfos = RuleService.getInstance().getWbInfos();
        //产生的告警消息列表
        CaseInfoModel cim=null;
        if(rule.getBus_type()==1){
            cim= RuleService.getInstance().getCaseInfoById(Integer.valueOf(rule.getBus_id()));
        }
        if (cim != null && cim.getUser_ids() != null) {
            String users = cim.getUser_ids();
            String[] uids = users.split(",");
            if (message.getType() == Msg.wj_dw_cdr) {
                Map hd = message.getContext();
//                if (supects != null && supects.size() > 0) {
//                    for (SupectModel sm : supects) {
//                        if (sm.getData_type() == SupectModel.DATA_TYPE_PHONE && sm.getData_value().equals(hd.get("phone").toString())) {
                //获取之前的状态
                StateModel state = RedisService.getInstance().getState(hd.get("phone").toString() + "@@" + rule.getId());
                for (KeyAreaModel area : areas) {
                    int last_r = 0;
                    if (state != null) {
                        last_r = inOrOut(area, state.getLat(), state.getLng());
                    }
                    int r = inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                    if (r == 1 && (last_r == -1 || last_r == 0)) { //进入
                        String p = RedisService.getInstance().get(String.valueOf(area.getId() + "@@" + rule.getId()));
                        if (p == null) p = "0";
                        Long peoples = Long.valueOf(p);
                        tempCache.put(String.valueOf(area.getId() + "@@" + rule.getId()), String.valueOf(peoples + 1));
                        if (Long.valueOf(rule.getRule_vlaue()) <= (peoples + 1)) {
                            for (String uid : uids) {
                                if (Strings.isNullOrEmpty(uid)) continue;
                                String msg = "区域:" + area.getName() + "[" + area.getId() + "]" + "人数到达" + String.valueOf(peoples + 1) + "人";
                                WarningModel warn = generate(rule.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                warns.add(warn);
                            }
                        }
                    } else if (r == -1 && (last_r == 0 || last_r == 1)) {  //离开
                        String p = RedisService.getInstance().get(String.valueOf(area.getId() + rule.getId()));
                        if (p != null) {
                            Long peoples = Long.valueOf(p);
                            tempCache.put(String.valueOf(area.getId() + rule.getId()), String.valueOf(peoples - 1));
                        }
                    }
                }
                //保存最新状态  把手机号关联的信息状态都保存
                StateModel s = new StateModel();
                s.setLat(Float.parseFloat(hd.get("lat").toString()));
                s.setLng(Float.parseFloat(hd.get("lng").toString()));
                s.setDate(hd.get("time").toString());
                tempCache.put(hd.get("phone").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                tempCache.put(hd.get("imei").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                tempCache.put(hd.get("imsi").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
//                        }
//                    }
//                }
            }
        }
        return warns;

    }

    /**
     * 聚集
     *
     * @param message
     * @param supects
     * @param areas
     * @param rule
     * @return
     */
    private static List<WarningModel> executeJj(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        Map<String, WbModel> wbInfos = RuleService.getInstance().getWbInfos();
        //挨个分析线索
        if (supects != null) {
            for (SupectModel sm : supects) {
                //线索对应的案件信息
                CaseInfoModel cim = null;
                if (rule.getBus_type() == 1) {
                    cim = RuleService.getInstance().getCaseInfoById(Integer.valueOf(rule.getBus_id()));
                }
                if (cim != null && cim.getUser_ids() != null) {
                    String users = cim.getUser_ids();
                    String[] uids = users.split(",");
                    switch (sm.getData_type()) {
                        case SupectModel.DATA_TYPE_PHONE:    //电话号码
                            //话单数据中有电话号码
                            if (message.getType() == Msg.wj_dw_cdr) {
                                Map hd = message.getContext();
                                if (sm.getData_value().equals(hd.get("phone"))) {
                                    logger.info("话单记录-手机号码相同:" + hd.get("phone"));
                                    //获取之前的状态
                                    StateModel state = RedisService.getInstance().getState(hd.get("phone").toString() + "@@" + rule.getId());
                                    for (KeyAreaModel area : areas) {
                                        int last_r = 0;
                                        if (state != null)
                                            last_r = inOrOut(area, state.getLat(), state.getLng());
                                        int r = inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                                        if (r == 1 && (last_r == -1 || last_r == 0)) { //进入
                                            //获取规则区域人数
                                            String renshu = RedisService.getInstance().get(area.getId() + String.valueOf(rule.getId()));
                                            if (renshu == null) renshu = "0";
                                            if (rule.getRule_type() == 1) {
                                                //所有人到指定区域才触发警报
                                                if ((Long.valueOf(renshu) + 1) == supects.size()) {
                                                    String msg = "总共" + Long.valueOf(renshu) + 1 + "人进入区域:" + area.getName() + "[" + area.getId() + "]";
                                                    for (String uid : uids) {
                                                        if (Strings.isNullOrEmpty(uid)) continue;
                                                        WarningModel warn = generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                        warns.add(warn);
                                                    }
                                                }
                                            } else {
                                                //到指定比例触发警报
                                                if ((Long.valueOf(renshu) + 1) >= supects.size() * Double.valueOf(rule.getRule_vlaue()) / 100) {
                                                    String msg = "总共" + Long.valueOf(renshu) + 1 + "人进入区域:" + area.getName() + "，比例为：" + Double.valueOf(rule.getRule_vlaue()) + "[" + area.getId() + "]";
                                                    for (String uid : uids) {
                                                        if (Strings.isNullOrEmpty(uid)) continue;
                                                        WarningModel warn = generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                        warns.add(warn);
                                                    }
                                                }
                                            }

                                            tempCache.put(area.getId() + String.valueOf(rule.getId()), String.valueOf(Integer.valueOf(renshu) + 1));
                                        } else if (r == -1 && (last_r == 0 || last_r == 1)) {  //离开
                                            String renshu = RedisService.getInstance().get(area.getId() + String.valueOf(rule.getId()));
                                            if (renshu != null) {
                                                tempCache.put(area.getId() + String.valueOf(rule.getId()), String.valueOf(Integer.valueOf(renshu) - 1));
                                            }
                                        }
                                    }

                                    //保存最新状态  把手机号关联的信息状态都保存
                                    StateModel s = new StateModel();
                                    s.setLat(Float.parseFloat(hd.get("lat").toString()));
                                    s.setLng(Float.parseFloat(hd.get("lng").toString()));
                                    s.setDate(hd.get("time").toString());
                                    tempCache.put(hd.get("phone").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                    tempCache.put(hd.get("imei").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                    tempCache.put(hd.get("imsi").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                }
                            } else if (message.getType() == Msg.wj_dw_sms) {
                                Map dx = message.getContext();
                                if (dx.get("phone").equals(sm.getData_value())) {
                                    logger.info("短信记录 - 手机号码相同:" + dx.get("phone"));
                                    //获取之前的状态
                                    StateModel state = RedisService.getInstance().getState(dx.get("phone").toString() + "@@" + rule.getId());

                                    for (KeyAreaModel area : areas) {
                                        int last_r = 0;
                                        if (state != null)
                                            last_r = inOrOut(area, state.getLat(), state.getLng());

                                        int r = inOrOut(area, Float.parseFloat(dx.get("lat").toString()), Float.parseFloat(dx.get("lng").toString()));
                                        if (r == 1 && (last_r == 0 || last_r == -1)) { //进入
                                            //获取规则区域人数
                                            String renshu = RedisService.getInstance().get(area.getId() + String.valueOf(rule.getId()));
                                            if (renshu == null) renshu = "0";
                                            if (rule.getRule_type() == 1) {
                                                //所有人到指定区域才触发警报
                                                if ((Long.valueOf(renshu) + 1) == supects.size()) {
                                                    String msg = "总共" + Long.valueOf(renshu) + 1 + "人进入区域:" + area.getName() + "[" + area.getId() + "]";
                                                    for (String uid : uids) {
                                                        if (Strings.isNullOrEmpty(uid)) continue;
                                                        WarningModel warn = generate(sm.getBus_id(), dx.get("sendtime").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                        warns.add(warn);
                                                    }
                                                }
                                            } else {
                                                //到指定比例触发警报
                                                if ((Long.valueOf(renshu) + 1) >= supects.size() * Double.valueOf(rule.getRule_vlaue()) / 100) {
                                                    String msg = "总共" + Long.valueOf(renshu) + 1 + "人进入区域:" + area.getName() + "，比例为：" + Double.valueOf(rule.getRule_vlaue()) + "[" + area.getId() + "]";
                                                    for (String uid : uids) {
                                                        if (Strings.isNullOrEmpty(uid)) continue;
                                                        WarningModel warn = generate(sm.getBus_id(), dx.get("sendtime").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                        warns.add(warn);
                                                    }
                                                }
                                            }

                                            if (renshu == null) renshu = "0";
                                            tempCache.put(area.getId() + String.valueOf(rule.getId()), String.valueOf(Integer.valueOf(renshu) + 1));
                                        }
                                        if (r == -1 && (last_r == 0 || last_r == 1)) { //离开
                                            String renshu = RedisService.getInstance().get(area.getId() + String.valueOf(rule.getId()));
                                            if (renshu != null) {
                                                tempCache.put(area.getId() + String.valueOf(rule.getId()), String.valueOf(Integer.valueOf(renshu) - 1));
                                            }
                                        }
                                    }

                                    //保存最新状态  把手机号关联的信息状态都保存
                                    StateModel s = new StateModel();
                                    s.setLat(Float.parseFloat(dx.get("lat").toString()));
                                    s.setLng(Float.parseFloat(dx.get("lng").toString()));
                                    s.setDate(dx.get("sendtime").toString());
                                    tempCache.put(dx.get("phone").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                    tempCache.put(dx.get("imei").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                    tempCache.put(dx.get("imsi").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                }
                            }
                            break;
                        case SupectModel.DATA_TYPE_IMEI:  //IMEI
                            //话单数据中有IMEI号码
                            if (message.getType() == Msg.wj_dw_cdr) {
                                Map hd = message.getContext();
                                if (sm.getData_value().equals(hd.get("imei").toString())) {
                                    logger.info("话单记录 - IMEI号码相同:" + hd.get("imei").toString());

                                    //获取之前的状态
                                    StateModel state = RedisService.getInstance().getState(hd.get("imei").toString() + "@@" + rule.getId());

                                    for (KeyAreaModel area : areas) {

                                        int last_r = 0;
                                        if (state != null)
                                            last_r = inOrOut(area, state.getLat(), state.getLng());

                                        int r = inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                                        if (r == 1 && (last_r == 0 || last_r == -1)) { //进入
                                            //获取规则区域人数
                                            String renshu = RedisService.getInstance().get(area.getId() + String.valueOf(rule.getId()));
                                            if (renshu == null) renshu = "0";
                                            if (rule.getRule_type() == 1) {
                                                //所有人到指定区域才触发警报
                                                if ((Long.valueOf(renshu) + 1) == supects.size()) {
                                                    String msg = "总共" + Long.valueOf(renshu) + 1 + "人进入区域:" + area.getName() + "[" + area.getId() + "]";
                                                    for (String uid : uids) {
                                                        if (Strings.isNullOrEmpty(uid)) continue;
                                                        WarningModel warn = generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                        warns.add(warn);
                                                    }
                                                }
                                            } else {
                                                //到指定比例触发警报
                                                if ((Long.valueOf(renshu) + 1) >= supects.size() * Double.valueOf(rule.getRule_vlaue()) / 100) {
                                                    String msg = "总共" + Long.valueOf(renshu) + 1 + "人进入区域:" + area.getName() + "，比例为：" + Double.valueOf(rule.getRule_vlaue()) + "[" + area.getId() + "]";
                                                    for (String uid : uids) {
                                                        if (Strings.isNullOrEmpty(uid)) continue;
                                                        WarningModel warn = generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                        warns.add(warn);
                                                    }
                                                }
                                            }

                                            if (renshu == null) renshu = "0";
                                            tempCache.put(area.getId() + String.valueOf(rule.getId()), String.valueOf(Integer.valueOf(renshu) + 1));
                                        }
                                        if (r == -1 && (last_r == 0 || last_r == 1)) { //离开
                                            String renshu = RedisService.getInstance().get(area.getId() + String.valueOf(rule.getId()));
                                            if (renshu != null) {
                                                tempCache.put(area.getId() + String.valueOf(rule.getId()), String.valueOf(Integer.valueOf(renshu) - 1));
                                            }
                                        }

                                    }
                                    //保存最新状态  把手机号关联的信息状态都保存
                                    StateModel s = new StateModel();
                                    s.setLat(Float.parseFloat(hd.get("lat").toString()));
                                    s.setLng(Float.parseFloat(hd.get("lng").toString()));
                                    s.setDate(hd.get("time").toString());
                                    tempCache.put(hd.get("phone").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                    tempCache.put(hd.get("imei").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                    tempCache.put(hd.get("imsi").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));

                                }
                            } else if (message.getType() == Msg.wj_dw_sms) {   //短信数据中有IMEI号码
                                Map dx = message.getContext();
                                if (dx.get("imei").toString().equals(sm.getData_value())) {
                                    logger.info("短信记录 - IMEI号码相同:" + dx.get("imei").toString());

                                    //获取之前的状态
                                    StateModel state = RedisService.getInstance().getState(dx.get("imei").toString() + "@@" + rule.getId());

                                    for (KeyAreaModel area : areas) {
                                        int last_r = 0;
                                        if (state != null)
                                            last_r = inOrOut(area, state.getLat(), state.getLng());

                                        int r = inOrOut(area, Float.parseFloat(dx.get("lat").toString()), Float.parseFloat(dx.get("lng").toString()));
                                        if (r == 1 && (last_r == 0 || last_r == -1)) { //进入
                                            //获取规则区域人数
                                            String renshu = RedisService.getInstance().get(area.getId() + String.valueOf(rule.getId()));
                                            if (renshu == null) renshu = "0";
                                            if (rule.getRule_type() == 1) {
                                                //所有人到指定区域才触发警报
                                                if ((Long.valueOf(renshu) + 1) == supects.size()) {
                                                    String msg = "总共" + Long.valueOf(renshu) + 1 + "人进入区域:" + area.getName() + "[" + area.getId() + "]";
                                                    for (String uid : uids) {
                                                        if (Strings.isNullOrEmpty(uid)) continue;
                                                        WarningModel warn = generate(sm.getBus_id(), dx.get("sendtime").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                        warns.add(warn);
                                                    }
                                                }
                                            } else {
                                                //到指定比例触发警报
                                                if ((Long.valueOf(renshu) + 1) >= supects.size() * Double.valueOf(rule.getRule_vlaue()) / 100) {
                                                    String msg = "总共" + Long.valueOf(renshu) + 1 + "人进入区域:" + area.getName() + "，比例为：" + Double.valueOf(rule.getRule_vlaue()) + "[" + area.getId() + "]";
                                                    for (String uid : uids) {
                                                        if (Strings.isNullOrEmpty(uid)) continue;
                                                        WarningModel warn = generate(sm.getBus_id(), dx.get("sendtime").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                        warns.add(warn);
                                                    }
                                                }
                                            }

                                            if (renshu == null) renshu = "0";
                                            tempCache.put(area.getId() + String.valueOf(rule.getId()), String.valueOf(Integer.valueOf(renshu) + 1));
                                        }
                                        if (r == -1 && (last_r == 0 || last_r == 1)) { //离开
                                            String renshu = RedisService.getInstance().get(area.getId() + String.valueOf(rule.getId()));
                                            if (renshu != null) {
                                                tempCache.put(area.getId() + String.valueOf(rule.getId()), String.valueOf(Integer.valueOf(renshu) - 1));
                                            }
                                        }
                                    }

                                    //保存最新状态  把手机号关联的信息状态都保存
                                    StateModel s = new StateModel();
                                    s.setLat(Float.parseFloat(dx.get("lat").toString()));
                                    s.setLng(Float.parseFloat(dx.get("lng").toString()));
                                    s.setDate(dx.get("sendtime").toString());
                                    tempCache.put(dx.get("phone").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                    tempCache.put(dx.get("imei").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                    tempCache.put(dx.get("imsi").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                }

                            }
                            break;
                        case SupectModel.DATA_TYPE_IMSI: //IMSI
                            //话单数据中有IMSI号码
                            if (message.getType() == Msg.wj_dw_cdr) {
                                Map hd = message.getContext();
                                if (sm.getData_value().equals(hd.get("imsi").toString())) {
                                    logger.info("话单记录 - IMSI号码相同:" + hd.get("imsi").toString());

                                    //获取之前的状态
                                    StateModel state = RedisService.getInstance().getState(hd.get("imsi").toString() + "@@" + rule.getId());

                                    for (KeyAreaModel area : areas) {

                                        int last_r = 0;
                                        if (state != null)
                                            last_r = inOrOut(area, state.getLat(), state.getLng());

                                        int r = inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                                        if (r == 1 && (last_r == 0 || last_r == -1)) { //进入
                                            //获取规则区域人数
                                            String renshu = RedisService.getInstance().get(area.getId() + String.valueOf(rule.getId()));
                                            if (renshu == null) renshu = "0";
                                            if (rule.getRule_type() == 1) {
                                                //所有人到指定区域才触发警报
                                                if ((Long.valueOf(renshu) + 1) == supects.size()) {
                                                    String msg = "总共" + Long.valueOf(renshu) + 1 + "人进入区域:" + area.getName() + "[" + area.getId() + "]";
                                                    for (String uid : uids) {
                                                        if (Strings.isNullOrEmpty(uid)) continue;
                                                        WarningModel warn = generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                        warns.add(warn);
                                                    }
                                                }
                                            } else {
                                                //到指定比例触发警报
                                                if ((Long.valueOf(renshu) + 1) >= supects.size() * Double.valueOf(rule.getRule_vlaue()) / 100) {
                                                    String msg = "总共" + Long.valueOf(renshu) + 1 + "人进入区域:" + area.getName() + "，比例为：" + Double.valueOf(rule.getRule_vlaue()) + "[" + area.getId() + "]";
                                                    for (String uid : uids) {
                                                        if (Strings.isNullOrEmpty(uid)) continue;
                                                        WarningModel warn = generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                        warns.add(warn);
                                                    }
                                                }
                                            }

                                            if (renshu == null) renshu = "0";
                                            tempCache.put(area.getId() + String.valueOf(rule.getId()), String.valueOf(Integer.valueOf(renshu) + 1));
                                        }
                                        if (r == -1 && (last_r == 0 || last_r == 1)) { //离开
                                            String renshu = RedisService.getInstance().get(area.getId() + String.valueOf(rule.getId()));
                                            if (renshu != null) {
                                                tempCache.put(area.getId() + String.valueOf(rule.getId()), String.valueOf(Integer.valueOf(renshu) - 1));
                                            }
                                        }

                                    }
                                    //保存最新状态  把手机号关联的信息状态都保存
                                    StateModel s = new StateModel();
                                    s.setLat(Float.parseFloat(hd.get("lat").toString()));
                                    s.setLng(Float.parseFloat(hd.get("lng").toString()));
                                    s.setDate(hd.get("time").toString());
                                    tempCache.put(hd.get("phone").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                    tempCache.put(hd.get("imei").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                    tempCache.put(hd.get("imsi").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));

                                }
                            } else if (message.getType() == Msg.wj_dw_sms) {   //短信数据中有IMSI号码
                                Map dx = message.getContext();
                                if (dx.get("imsi").toString().equals(sm.getData_value())) {
                                    logger.info("短信记录 - IMSI号码相同:" + dx.get("imsi").toString());
                                    //获取之前的状态
                                    StateModel state = RedisService.getInstance().getState(dx.get("imsi").toString() + "@@" + rule.getId());
                                    for (KeyAreaModel area : areas) {
                                        int last_r = 0;
                                        if (state != null)
                                            last_r = inOrOut(area, state.getLat(), state.getLng());

                                        int r = inOrOut(area, Float.parseFloat(dx.get("lat").toString()), Float.parseFloat(dx.get("lng").toString()));
                                        if (r == 1 && (last_r == 0 || last_r == -1)) { //进入
                                            //获取规则区域人数
                                            String renshu = RedisService.getInstance().get(area.getId() + String.valueOf(rule.getId()));
                                            if (renshu == null) renshu = "0";
                                            if (rule.getRule_type() == 1) {
                                                //所有人到指定区域才触发警报
                                                if ((Long.valueOf(renshu) + 1) == supects.size()) {
                                                    String msg = "总共" + Long.valueOf(renshu) + 1 + "人进入区域:" + area.getName() + "[" + area.getId() + "]";
                                                    for (String uid : uids) {
                                                        if (Strings.isNullOrEmpty(uid)) continue;
                                                        WarningModel warn = generate(sm.getBus_id(), dx.get("sendtime").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                        warns.add(warn);
                                                    }
                                                }
                                            } else {
                                                //到指定比例触发警报
                                                if ((Long.valueOf(renshu) + 1) >= supects.size() * Double.valueOf(rule.getRule_vlaue()) / 100) {
                                                    String msg = "总共" + Long.valueOf(renshu) + 1 + "人进入区域:" + area.getName() + "，比例为：" + Double.valueOf(rule.getRule_vlaue()) + "[" + area.getId() + "]";
                                                    for (String uid : uids) {
                                                        if (Strings.isNullOrEmpty(uid)) continue;
                                                        WarningModel warn = generate(sm.getBus_id(), dx.get("sendtime").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                        warns.add(warn);
                                                    }
                                                }
                                            }

                                            if (renshu == null) renshu = "0";
                                            tempCache.put(area.getId() + String.valueOf(rule.getId()), String.valueOf(Integer.valueOf(renshu) + 1));
                                        }
                                        if (r == -1 && (last_r == 0 || last_r == 1)) { //离开
                                            String renshu = RedisService.getInstance().get(area.getId() + String.valueOf(rule.getId()));
                                            if (renshu != null) {
                                                tempCache.put(area.getId() + String.valueOf(rule.getId()), String.valueOf(Integer.valueOf(renshu) - 1));
                                            }
                                        }
                                    }
                                    //保存最新状态  把手机号关联的信息状态都保存
                                    StateModel s = new StateModel();
                                    s.setLat(Float.parseFloat(dx.get("lat").toString()));
                                    s.setLng(Float.parseFloat(dx.get("lng").toString()));
                                    s.setDate(dx.get("sendtime").toString());
                                    tempCache.put(dx.get("phone").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                    tempCache.put(dx.get("imei").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                    tempCache.put(dx.get("imsi").toString() + "@@" + rule.getId(), JSONObject.toJSONString(s));
                                }
                            }
                            break;
                        case SupectModel.DATA_TYPE_CARD: //身份证号
                            if (message.getType() == Msg.wj_net_use) {
                                Map wb = message.getContext();
                                if (sm.getData_value().equals(wb.get("id_card").toString())) {
                                    //获取之前的状态
                                    StateModel state = RedisService.getInstance().getState(wb.get("id_card").toString() + "@@" + rule.getId());
                                    for (KeyAreaModel area : areas) {
                                        int last_r = 0;
                                        if (state != null) {
                                            last_r = inOrOut(area, state.getLat(), state.getLng());
                                        }
                                        WbModel wbm = wbInfos.get(wb.get("net_code").toString());
                                        int r = inOrOut(area, Float.parseFloat(wbm.getLatitude()), Float.parseFloat(wbm.getLongitude()));
                                        if (r == 1 && (last_r == -1 || last_r == 0)) { //进入
                                            //获取规则区域人数
                                            String renshu = RedisService.getInstance().get(area.getId() + String.valueOf(rule.getId()));
                                            if (renshu == null) renshu = "0";
                                            if (rule.getRule_type() == 1) {
                                                //所有人到指定区域才触发警报
                                                if ((Long.valueOf(renshu) + 1) == supects.size()) {
                                                    String msg = "总共" + Long.valueOf(renshu) + 1 + "人进入区域:" + area.getName() + "[" + area.getId() + "]";
                                                    for (String uid : uids) {
                                                        if (Strings.isNullOrEmpty(uid)) continue;
                                                        WarningModel warn = generate(sm.getBus_id(), wb.get("up_time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                        warns.add(warn);
                                                    }
                                                }
                                            } else {
                                                //到指定比例触发警报
                                                if ((Long.valueOf(renshu) + 1) >= supects.size() * Double.valueOf(rule.getRule_vlaue()) / 100) {
                                                    String msg = "总共" + Long.valueOf(renshu) + 1 + "人进入区域:" + area.getName() + "，比例为：" + Double.valueOf(rule.getRule_vlaue()) + "[" + area.getId() + "]";
                                                    for (String uid : uids) {
                                                        if (Strings.isNullOrEmpty(uid)) continue;
                                                        WarningModel warn = generate(sm.getBus_id(), wb.get("up_time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                                        warns.add(warn);
                                                    }
                                                }
                                            }

                                            if (renshu == null) renshu = "0";
                                            tempCache.put(area.getId() + String.valueOf(rule.getId()), String.valueOf(Integer.valueOf(renshu) + 1));
                                        } else if (r == -1 && (last_r == 0 || last_r == 1)) { //离开
                                            String renshu = RedisService.getInstance().get(area.getId() + String.valueOf(rule.getId()));
                                            if (renshu != null) {
                                                tempCache.put(area.getId() + String.valueOf(rule.getId()), String.valueOf(Integer.valueOf(renshu) - 1));
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
            }
        }
        return warns;

    }

    /**
     * 失联
     *
     * @param message
     * @param supects
     * @param areas
     * @param rule
     * @return
     */
    private static List<WarningModel> executeSl(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Map<String, WbModel> wbInfos = RuleService.getInstance().getWbInfos();
        //挨个分析线索
        for (SupectModel sm : supects) {
            //线索对应的案件信息
            CaseInfoModel cim = null;
            if (rule.getBus_type() == 1) {
                cim = RuleService.getInstance().getCaseInfoById(Integer.valueOf(rule.getBus_id()));
            }
            String users = cim.getUser_ids();
            String[] uids = users.split(",");
            switch (sm.getData_type()) {
                case SupectModel.DATA_TYPE_PHONE:    //电话号码
                    //话单数据中有电话号码
                    if (message.getType() == Msg.wj_dw_cdr) {
                        Map hd = message.getContext();
                        if (sm.getData_value().equals(hd.get("phone"))) {
                            logger.info("话单记录-手机号码相同:" + hd.get("phone"));
                            tempCache.put(hd.get("phone").toString() + "@@" + rule.getId(), hd.get("time").toString());
                        }
                    } else if (message.getType() == Msg.wj_dw_sms) {
                        Map dx = message.getContext();
                        if (dx.get("phone").equals(sm.getData_value())) {
                        }
                    } else if (message.getType() == Msg.szjc) {
                        try {
                            Date d = sdf.parse(RedisService.getInstance().get(sm.getData_value() + "@@" + rule.getId()));
                            if (System.currentTimeMillis() - d.getTime() > Long.valueOf(rule.getRule_vlaue()) * 3600 * 1000) {
                                String msg = sm.getData_value() + "失联" + rule.getRule_vlaue() + "小时";
                                for (String uid : uids) {
                                    WarningModel warn = generate(sm.getBus_id(), sdf.format(new Date()), msg, rule.getId(), -1, uid);
                                    warns.add(warn);
                                }
                            }
                        } catch (Exception e) {
                            //TODO
                            e.printStackTrace();
                        }
                    }
                    break;
                case SupectModel.DATA_TYPE_IMEI:  //IMEI
                    //话单数据中有IMEI号码
                    if (message.getType() == Msg.wj_dw_cdr) {
                        Map hd = message.getContext();
                        if (sm.getData_value().equals(hd.get("imei").toString())) {
                        }

                    } else if (message.getType() == Msg.wj_dw_sms) {   //短信数据中有IMEI号码
                        Map dx = message.getContext();
                        if (dx.get("imei").toString().equals(sm.getData_value())) {
                        }

                    }
                    break;
                case SupectModel.DATA_TYPE_IMSI: //IMSI
                    //话单数据中有IMSI号码
                    if (message.getType() == Msg.wj_dw_cdr) {
                        Map hd = message.getContext();
                        if (sm.getData_value().equals(hd.get("imsi").toString())) {
                        }
                    } else if (message.getType() == Msg.wj_dw_sms) {   //短信数据中有IMSI号码
                        Map dx = message.getContext();
                        if (dx.get("imsi").toString().equals(sm.getData_value())) {
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


    private static void executeEx(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

        switch (rule.getRule_vlaue()) {
            case WarningRuleModel.EX_TH_2:
                //异常类型1（当天拨打电话超过2小时）
                ExceptionCompareUtil.executeExTh2(message, supects, areas, rule, warns, tempCache);
                break;
            //当天凌晨0-4点通话超过10次
            case WarningRuleModel.EX_TH_10:
                ExceptionCompareUtil.executeExTh10(message, supects, areas, rule, warns, tempCache);
                break;
            //当天凌晨0-4点拨打特定号码通话超过5次
            case WarningRuleModel.EX_TH_5:
                ExceptionCompareUtil.executeEX_TH_5(message, supects, areas, rule, warns, tempCache);
                break;
            //平均每月寄出快递数据超过15次
            case WarningRuleModel.EX_KD_15:
                ExceptionCompareUtil.executeEX_KD_15(message, supects, areas, rule, warns, tempCache);
                break;
            case WarningRuleModel.EX_KD_30:
                ExceptionCompareUtil.executeEX_KD_30(message, supects, areas, rule, warns, tempCache);
                break;
            case WarningRuleModel.EX_WB_20:
                ExceptionCompareUtil.executeEX_WB_20(message, supects, areas, rule, warns, tempCache);
                break;
            case WarningRuleModel.EX_WB_15:
                ExceptionCompareUtil.executeEX_WB_15(message, supects, areas, rule, warns, tempCache);
                break;
            case WarningRuleModel.EX_WB_3FC:
                ExceptionCompareUtil.executeEX_WB_3FC(message, supects, areas, rule, warns, tempCache);
                break;
            case WarningRuleModel.EX_TH_3FC:
                ExceptionCompareUtil.executeEX_TH_3FC(message, supects, areas, rule, warns, tempCache);
                break;
            case WarningRuleModel.EX_TH_0_4_3FC:
                ExceptionCompareUtil.executeEX_TH_0_4_3FC(message, supects, areas, rule, warns, tempCache);
                break;
            case WarningRuleModel.EX_TH_0_4_1:
                ExceptionCompareUtil.executeEX_TH_0_4_1(message, supects, areas, rule, warns, tempCache);
                break;
            case WarningRuleModel.EX_TH_0_4_TD3FC:
                ExceptionCompareUtil.executeEX_TH_0_4_TD3FC(message, supects, areas, rule, warns, tempCache);
                break;
            case WarningRuleModel.EX_TH_0_4_TD_1:
                ExceptionCompareUtil.executeEX_TH_0_4_TD_1(message, supects, areas, rule, warns, tempCache);
                break;
            case WarningRuleModel.EX_THCS_3FC:
                ExceptionCompareUtil.executeEX_THCS_3FC(message, supects, areas, rule, warns, tempCache);
                break;
            case WarningRuleModel.EX_THCS_0_4_3FC:
                ExceptionCompareUtil.executeEX_THCS_0_4_3FC(message, supects, areas, rule, warns, tempCache);
                break;
            case WarningRuleModel.EX_THCS_5:
                ExceptionCompareUtil.executeEX_THCS_5(message, supects, areas, rule, warns, tempCache);
                break;
            case WarningRuleModel.EX_THCS_0_4_TD3FC:
                ExceptionCompareUtil.executeEX_THCS_0_4_TD3FC(message, supects, areas, rule, warns, tempCache);
                break;
            case WarningRuleModel.EX_KD_3FC:
                ExceptionCompareUtil.executeEX_KD_3FC(message, supects, areas, rule, warns, tempCache);
                break;
        }

    }

    /**
     * 判断当前位置是否在区域内
     *
     * @param area 区域
     * @param lat  纬度
     * @param lng  经度
     * @return 1-进入了   -1-离开了 0-检测失败
     */
    public static int inOrOut(KeyAreaModel area, float lat, float lng) {
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

    /**
     * @param bus_id      业务ID
     * @param begine_time 发生时间
     * @param msg
     * @param rule_id
     * @param type
     * @param uid
     * @return
     */
    public static WarningModel generate(int bus_id, String begine_time, String msg, Long rule_id, int type, String uid) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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
        warn.setIsAttention(2);
        return warn;
    }

    /**
     * 隐性异常挖掘
     *
     * @param begine_time
     * @param msg
     * @param rule_id
     * @param type
     * @param uid
     * @return
     */
    public static WarningModel generate2(String begine_time, String msg, Long rule_id, int type, String uid) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        WarningModel warn = new WarningModel();
        warn.setId(UUID.randomUUID().toString());
        warn.setBuinesstype(2);  //默认1
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
        warn.setIsAttention(2);
        return warn;
    }

}
