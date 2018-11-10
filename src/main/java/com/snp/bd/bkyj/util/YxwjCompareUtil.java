package com.snp.bd.bkyj.util;

import com.alibaba.fastjson.JSONObject;
import com.snp.bd.bkyj.model.*;
import com.snp.bd.bkyj.redis.RedisService;
import com.snp.bd.bkyj.rule.RuleCompareService;
import com.snp.bd.bkyj.rule.RuleService;
import org.apache.log4j.Logger;
import scala.util.parsing.json.JSON;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by x on 2018/5/31.
 */
public class YxwjCompareUtil {

    private static Logger logger = Logger.getLogger("隐性挖掘比对服务");
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void executeExTh2(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        //话单数据中有电话号码
        Map<String,Integer> param= com.alibaba.fastjson.JSON.parseObject(rule.getJSONPARAM(), Map.class);
        String[] uids = rule.getYHID().split(",");
        if (message.getType() == Msg.wj_dw_cdr) {
            Map hd = message.getContext();
            for (KeyAreaModel area : areas) {
                int inout = RuleCompareService.inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                if (inout == 1) {
                    Long dura = Long.valueOf(hd.get("dura").toString());
                    String mtdura = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId());
                    if (mtdura == null) mtdura = "0";
                    if (Long.valueOf(mtdura) + dura > param.get("paramone") * 3600) {
                        String msg = hd.get("phone") + "通话时间超过2小时";
                        for (String uid : uids) {
                            WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                            warns.add(warn);
                        }
                    }
                    tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtdura) + dura));
                }
            }
        }
    }

    public static void executeEX_TH_0_4_1(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        //话单数据中有电话号码
        Map<String,Integer> param= com.alibaba.fastjson.JSON.parseObject(rule.getJSONPARAM(), Map.class);
        String[] uids = rule.getYHID().split(",");
        if (message.getType() == Msg.wj_dw_cdr) {
            Map hd = message.getContext();
            Long hour = Long.valueOf(hd.get("time").toString().split(" ")[1].split(":")[0]);
            if ( param.get("paramone") < hour && hour <  param.get("paramtwo")) {
                for (KeyAreaModel area : areas) {
                    int inout = RuleCompareService.inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                    if (inout == 1) {
                        Long dura = Long.valueOf(hd.get("dura").toString());
                        String mtdura = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId());
                        if (mtdura == null) mtdura = "0";
                        if (Long.valueOf(mtdura) + dura >  param.get("paramthree") * 3600) {
                            String msg = hd.get("phone") + " 0-4点通话时间超过1小时";
                            for (String uid : uids) {
                                WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                warns.add(warn);
                            }
                        }
                        tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtdura) + dura));
                    }
                }
            }
        }
    }

    public static void executeEX_TH_0_4_3FC(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        String[] uids = rule.getYHID().split(",");
        if (message.getType() == Msg.wj_dw_cdr) {
            Map hd = message.getContext();
            Long hour = Long.valueOf(hd.get("time").toString().split(" ")[1].split(":")[0]);
            if (0 < hour && hour < 4) {
                for (KeyAreaModel area : areas) {
                    int inout = RuleCompareService.inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                    if (inout == 1) {
                        Long dura = Long.valueOf(hd.get("dura").toString());
                        String mtdura = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId());
                        if (mtdura == null) mtdura = "0";
                        if (Long.valueOf(mtdura) + dura - HbaseService.getAvgThSc0_4(hd.get("phone").toString()) > 3 * HbaseService.getStdThSc0_4(hd.get("phone").toString())) {
                            String msg = hd.get("phone") + "当天0-4点拨打电话通话总时间超过过去一个月3倍方差";
                            for (String uid : uids) {
                                WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                warns.add(warn);
                            }
                        }
                        tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtdura) + dura));
                    }
                }
            }
        }
    }

    public static void executeEX_THCS_0_4_3FC(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        String[] uids = rule.getYHID().split(",");
        if (message.getType() == Msg.wj_dw_cdr) {
            Map hd = message.getContext();
            Long hour = Long.valueOf(hd.get("time").toString().split(" ")[1].split(":")[0]);
            if (0 < hour && hour < 4) {
                for (KeyAreaModel area : areas) {
                    int inout = RuleCompareService.inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                    if (inout == 1) {
                        logger.info("话单记录-手机号码相同:" + hd.get("phone"));
                        String mtdura = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId());
                        if (mtdura == null) mtdura = "0";
                        if (Long.valueOf(mtdura) + 1 - HbaseService.getAvgThCs0_4(hd.get("phone").toString()) > 3 * HbaseService.getStdThCs0_4(hd.get("phone").toString())) {
                            String msg = hd.get("phone") + "当天0-4点拨打电话次数超过过去一个月3倍方差";
                            for (String uid : uids) {
                                WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                warns.add(warn);
                            }
                        }
                        tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtdura) + 1));
                    }
                }
            }
        }
    }

    public static void executeEX_TH_3FC(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        //话单数据中有电话号码
        String[] uids = rule.getYHID().split(",");
        if (message.getType() == Msg.wj_dw_cdr) {
            Map hd = message.getContext();
            for (KeyAreaModel area : areas) {
                int inout = RuleCompareService.inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                if (inout == 1) {
                    Long dura = Long.valueOf(hd.get("dura").toString());
                    String mtdura = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId());
                    if (mtdura == null) mtdura = "0";
                    if (Long.valueOf(mtdura) + dura - HbaseService.getAvgThSc(hd.get("phone").toString()) > 3 * HbaseService.getStdThSc(hd.get("phone").toString())) {
                        String msg = hd.get("phone") + "当天拨打电话通话总时间超过过去一个月3倍方差";
                        for (String uid : uids) {
                            WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                            warns.add(warn);
                        }
                    }
                    tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtdura) + dura));
                }
            }
        }
    }

    public static void executeEX_THCS_3FC(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        String[] uids = rule.getYHID().split(",");
        if (message.getType() == Msg.wj_dw_cdr) {
            Map hd = message.getContext();
            for (KeyAreaModel area : areas) {
                int inout = RuleCompareService.inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                if (inout == 1) {
                    String mtdura = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId());
                    if (mtdura == null) mtdura = "0";
                    if (Long.valueOf(mtdura) + 1 - HbaseService.getAvgThCs(hd.get("phone").toString()) > 3 * HbaseService.getStdThCs(hd.get("phone").toString())) {
                        String msg = hd.get("phone") + "当天拨打电话次数超过过去一个月3倍方差";
                        for (String uid : uids) {
                            WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                            warns.add(warn);
                        }
                    }
                    tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtdura) + 1));
                }
            }
        }
    }

    public static void executeExTh10(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        String[] uids = rule.getYHID().split(",");
        if (message.getType() == Msg.wj_dw_cdr) {
            Map hd = message.getContext();
            Long hour = Long.valueOf(hd.get("time").toString().split(" ")[1].split(":")[0]);
            if (0 < hour && hour < 4) {
                for (KeyAreaModel area : areas) {
                    int inout = RuleCompareService.inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                    if (inout == 1) {
                        String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId());
                        if (mtthcs == null) mtthcs = "0";
                        if (Long.valueOf(mtthcs) + 1 > 10) {
                            String msg = hd.get("phone") + " 0-4点通话次数超过10次";
                            for (String uid : uids) {
                                WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                warns.add(warn);
                            }
                        }
                        tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtthcs) + 1));
                    }
                }
            }
        }
    }
    public static void executeEX_THCS_5(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        Map<String,Integer> param= com.alibaba.fastjson.JSON.parseObject(rule.getJSONPARAM(), Map.class);
        String[] uids = rule.getYHID().split(",");
        if (message.getType() == Msg.wj_dw_cdr) {
            Map hd = message.getContext();
                for (KeyAreaModel area : areas) {
                    int inout = RuleCompareService.inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                    if (inout == 1) {
                        String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId());
                        if (mtthcs == null) mtthcs = "0";
                        if (Long.valueOf(mtthcs) + 1 > param.get("paramone")) {
                            String msg = hd.get("phone") + " 通话次数超过"+ param.get("paramone");
                            for (String uid : uids) {
                                WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                warns.add(warn);
                            }
                        }
                        tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtthcs) + 1));
                    }
                }
        }
    }
    public static void executeEX_TH_5(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        String[] uids = rule.getYHID().split(",");
        Map<String, String> params = JSONObject.parseObject(rule.getJSONPARAM(), Map.class);
        if (message.getType() == Msg.wj_dw_cdr) {
            Map hd = message.getContext();
            Long hour = Long.valueOf(hd.get("time").toString().split(" ")[1].split(":")[0]);
            if (params.values().contains(hd.get("tarnum"))) {
                if (0 < hour && hour < 4) {
                    for (KeyAreaModel area : areas) {
                        int inout = RuleCompareService.inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                        if (inout == 1) {
                            String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId() + "@@" + hd.get("tarnum"));
                            if (mtthcs == null) mtthcs = "0";
                            if (Long.valueOf(mtthcs) + 1 > 5) {
                                String msg = hd.get("phone") + " 0-4点与" + hd.get("tarnum") + "通话次数超过5次";
                                for (String uid : uids) {
                                    WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                    warns.add(warn);
                                }
                            }
                            tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId() + "@@" + hd.get("tarnum"), String.valueOf(Long.valueOf(mtthcs) + 1));
                        }
                    }
                }
            }
        }
    }

    public static void executeEX_TH_0_4_TD3FC(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        String[] uids = rule.getYHID().split(",");
        Map<String, String> params = JSONObject.parseObject(rule.getJSONPARAM(), Map.class);
        if (message.getType() == Msg.wj_dw_cdr) {
            Map hd = message.getContext();
            Long hour = Long.valueOf(hd.get("time").toString().split(" ")[1].split(":")[0]);
            if (params.values().contains(hd.get("tarnum"))) {
                if (0 < hour && hour < 4) {
                    for (KeyAreaModel area : areas) {
                        int inout = RuleCompareService.inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                        if (inout == 1) {
                            logger.info("话单记录-手机号码相同:" + hd.get("phone"));
                            Long dura = Long.valueOf(hd.get("dura").toString());
                            String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId() + "@@" + hd.get("tarnum"));
                            if (mtthcs == null) mtthcs = "0";
                            if (Long.valueOf(mtthcs) + dura - HbaseService.getAvgThsc0_4(hd.get("phone").toString(), hd.get("tarnum").toString()) > HbaseService.getStdThsc0_4(hd.get("phone").toString(), hd.get("tarnum").toString())) {
                                String msg = hd.get("phone") + " 0-4点与" + hd.get("tarnum") + "通话时长超过过去一个月的3倍";
                                for (String uid : uids) {
                                    WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                    warns.add(warn);
                                }
                            }
                            tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId() + "@@" + hd.get("tarnum"), String.valueOf(Long.valueOf(mtthcs) + dura));
                        }
                    }
                }
            }
        }
    }
    public static void executeEX_TH_0_4_TD_1(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        String[] uids = rule.getYHID().split(",");
        Map<String, String> params = JSONObject.parseObject(rule.getJSONPARAM(), Map.class);
        if (message.getType() == Msg.wj_dw_cdr) {
            Map hd = message.getContext();
            Long hour = Long.valueOf(hd.get("time").toString().split(" ")[1].split(":")[0]);
            if (params.values().contains(hd.get("tarnum"))) {
                if (Integer.valueOf(params.get("paramone")) < hour && hour < Integer.valueOf(params.get("paramtwo"))) {
                    for (KeyAreaModel area : areas) {
                        int inout = RuleCompareService.inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                        if (inout == 1) {
                            logger.info("话单记录-手机号码相同:" + hd.get("phone"));
                            Long dura = Long.valueOf(hd.get("dura").toString());
                            String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId() + "@@" + hd.get("tarnum"));
                            if (mtthcs == null) mtthcs = "0";
                            if (Long.valueOf(mtthcs) + dura>Integer.valueOf(params.get("paramthree"))*3600 ) {
                                String msg = hd.get("phone") + params.get("paramone")+"-"+params.get("paramtwo")+"点与" + hd.get("tarnum") + "通话时长超过"+Integer.valueOf(params.get("paramthree"));
                                for (String uid : uids) {
                                    WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                    warns.add(warn);
                                }
                            }
                            tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId() + "@@" + hd.get("tarnum"), String.valueOf(Long.valueOf(mtthcs) + dura));
                        }
                    }
                }
            }
        }
    }
    public static void executeEX_THCS_0_4_TD3FC(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        String[] uids = rule.getYHID().split(",");
        Map<String, String> params = JSONObject.parseObject(rule.getJSONPARAM(), Map.class);
        if (message.getType() == Msg.wj_dw_cdr) {
            Map hd = message.getContext();
            Long hour = Long.valueOf(hd.get("time").toString().split(" ")[1].split(":")[0]);
            if (params.values().contains(hd.get("tarnum"))) {
                if (0 < hour && hour < 4) {
                    for (KeyAreaModel area : areas) {
                        int inout = RuleCompareService.inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                        if (inout == 1) {
                            String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId() + "@@" + hd.get("tarnum"));
                            if (mtthcs == null) mtthcs = "0";
                            if (Long.valueOf(mtthcs) + 1 - HbaseService.getAvgThsc0_4(hd.get("phone").toString(), hd.get("tarnum").toString()) > HbaseService.getStdThsc0_4(hd.get("phone").toString(), hd.get("tarnum").toString())) {
                                String msg = hd.get("phone") + " 0-4点与" + hd.get("tarnum") + "通话次数超过过去一个月的3倍";
                                for (String uid : uids) {
                                    WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                    warns.add(warn);
                                }
                            }
                            tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId() + "@@" + hd.get("tarnum"), String.valueOf(Long.valueOf(mtthcs) + 1));
                        }
                    }
                }
            }
        }
    }

    public static void executeEX_KD_3FC(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        //TODO 无法获取快递的gps坐标，后续完善(1暂时默认已转换成gps坐标)
        String[] uids = rule.getYHID().split(",");
        if (message.getType() == Msg.WJ_MD) {
            Map hd = message.getContext();
            for (KeyAreaModel area : areas) {
                int inout = RuleCompareService.inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                if (inout == 1) {
                    String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId());
                    if (mtthcs == null) mtthcs = "0";
                    if (Long.valueOf(mtthcs) + 1 - HbaseService.getAvgJdCs(hd.get("phone").toString()) > 3 * HbaseService.getStdJdCs(hd.get("phone").toString())) {
                        String msg = hd.get("phone") + " 平均每月寄出快递数据超过3倍月标准差";
                        for (String uid : uids) {
                            WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_JD, uid);
                            warns.add(warn);
                        }
                    }
                    tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtthcs) + 1));
                }
            }
        }
    }

    public static void executeEX_KD_15(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        //TODO 无法获取快递的gps坐标，后续完善(1暂时默认已转换成gps坐标)
        String[] uids = rule.getYHID().split(",");
        if (message.getType() == Msg.WJ_MD) {
            Map hd = message.getContext();
            for (KeyAreaModel area : areas) {
                int inout = RuleCompareService.inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                if (inout == 1) {
                    String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId());
                    if (mtthcs == null) mtthcs = "0";
                    if (Long.valueOf(mtthcs) + 1 > 15 && HbaseService.getAvgJdCs(hd.get("phone").toString()) > 15) {
                        String msg = hd.get("phone") + " 平均每月寄出快递数据超过15次";
                        for (String uid : uids) {
                            WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_JD, uid);
                            warns.add(warn);
                        }
                    }
                    tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtthcs) + 1));
                }
            }
        }
    }

    public static void executeEX_KD_30(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        //TODO 无法获取快递的gps坐标，后续完善(1暂时默认已转换成gps坐标)
        String[] uids = rule.getYHID().split(",");
        if (message.getType() == Msg.WJ_MD) {
            Map hd = message.getContext();
            for (KeyAreaModel area : areas) {
                int inout = RuleCompareService.inOrOut(area, Float.parseFloat(hd.get("lat").toString()), Float.parseFloat(hd.get("lng").toString()));
                if (inout == 1) {
                    String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId());
                    if (mtthcs == null) mtthcs = "0";
                    if (Long.valueOf(mtthcs) + 1 > 30) {
                        String msg = hd.get("phone") + " 当月寄出快递数据超过30次";
                        for (String uid : uids) {
                            WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_JD, uid);
                            warns.add(warn);
                        }
                    }
                    tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtthcs) + 1));
                }
            }
        }
    }

    public static void executeEX_WB_20(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        Map<String, WbModel> wbInfos = RuleService.getInstance().getWbInfos();
        String[] uids = rule.getYHID().split(",");
        if (message.getType() == Msg.wj_net_use) {
            Map hd = message.getContext();
            WbModel wbm = wbInfos.get(hd.get("net_code").toString());
            for (KeyAreaModel area : areas) {
                int inout = RuleCompareService.inOrOut(area, Float.parseFloat(wbm.getLatitude()), Float.parseFloat(wbm.getLongitude()));
                if (inout == 1) {
                    String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId());
                    if (mtthcs == null) mtthcs = "0";
                    if (Long.valueOf(mtthcs) + 1 > 20) {
                        String msg = hd.get("phone") + " 当月去网吧超过20次";
                        for (String uid : uids) {
                            WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_JD, uid);
                            warns.add(warn);
                        }
                    }
                    tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtthcs) + 1));
                }
            }
        }
    }
    public static void executeEX_WB_15(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        Map<String, WbModel> wbInfos = RuleService.getInstance().getWbInfos();
        Map<String,Integer> param= com.alibaba.fastjson.JSON.parseObject(rule.getJSONPARAM(), Map.class);
        String[] uids = rule.getYHID().split(",");
        if (message.getType() == Msg.wj_net_use) {
            Map hd = message.getContext();
            WbModel wbm = wbInfos.get(hd.get("net_code").toString());
            for (KeyAreaModel area : areas) {
                int inout = RuleCompareService.inOrOut(area, Float.parseFloat(wbm.getLatitude()), Float.parseFloat(wbm.getLongitude()));
                if (inout == 1) {
                    String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId());
                    if (mtthcs == null) mtthcs = "0";
                    if (Long.valueOf(mtthcs) + 1 > param.get("paramone")&&HbaseService.getAvgWbCs(hd.get("phone").toString()) >param.get("paramone")) {
                        String msg = hd.get("phone") + " 平均每月去网吧次数超过"+param.get("paramone");
                        for (String uid : uids) {
                            WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_JD, uid);
                            warns.add(warn);
                        }
                    }
                    tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtthcs) + 1));
                }
            }
        }
    }
    public static void executeEX_WB_3FC(Msg message, List<KeyAreaModel> areas, YxwjModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
        Map<String, WbModel> wbInfos = RuleService.getInstance().getWbInfos();
        Map<String,Integer> param= com.alibaba.fastjson.JSON.parseObject(rule.getJSONPARAM(), Map.class);
        String[] uids = rule.getYHID().split(",");
        if (message.getType() == Msg.wj_net_use) {
            Map hd = message.getContext();
            WbModel wbm = wbInfos.get(hd.get("net_code").toString());
            for (KeyAreaModel area : areas) {
                int inout = RuleCompareService.inOrOut(area, Float.parseFloat(wbm.getLatitude()), Float.parseFloat(wbm.getLongitude()));
                if (inout == 1) {
                    String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId());
                    if (mtthcs == null) mtthcs = "0";
                    if (Long.valueOf(mtthcs) + 1 -HbaseService.getAvgWbCs(hd.get("phone").toString()) >param.get("paramone")*HbaseService.getStdWbCs(hd.get("phone").toString())) {
                        String msg = hd.get("phone") + " 当月去网吧次数与均值的差超过过去一个月的"+param.get("paramone")+"倍标准差";
                        for (String uid : uids) {
                            WarningModel warn = RuleCompareService.generate2(hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_JD, uid);
                            warns.add(warn);
                        }
                    }
                    tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtthcs) + 1));
                }
            }
        }
    }
}
