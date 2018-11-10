package com.snp.bd.bkyj.util;

import com.snp.bd.bkyj.model.*;
import com.snp.bd.bkyj.redis.RedisService;
import com.snp.bd.bkyj.rule.RuleCompareService;
import com.snp.bd.bkyj.rule.RuleService;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by x on 2018/5/31.
 */
public class ExceptionCompareUtil {
    private static Logger logger = Logger.getLogger("异常比对服务");
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void executeExTh2(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {
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
                                Long dura = Long.valueOf(hd.get("dura").toString());
                                String mtdura = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId());
                                if (mtdura == null) mtdura = "0";
                                if (Long.valueOf(mtdura) + dura > 2 * 3600) {
                                    String msg = hd.get("phone") + "通话时间超过2小时";
                                    for (String uid : uids) {
                                        WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                        warns.add(warn);
                                    }
                                }
                                tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtdura) + dura));
                            }
                        }
                }
            }
        }
    }

    public static void executeEX_TH_0_4_3FC(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

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
                                Long hour = Long.valueOf(hd.get("time").toString().split(" ")[1].split(":")[0]);
                                if (0 < hour && hour < 4) {
                                    logger.info("话单记录-手机号码相同:" + hd.get("phone"));
                                    Long dura = Long.valueOf(hd.get("dura").toString());
                                    String mtdura = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId());
                                    if (mtdura == null) mtdura = "0";
                                    if (Long.valueOf(mtdura) + dura - HbaseService.getAvgThSc0_4(hd.get("phone").toString()) > 3 * HbaseService.getStdThSc0_4(hd.get("phone").toString())) {
                                        String msg = hd.get("phone") + "当天0-4点拨打电话通话总时间超过过去一个月3倍方差";
                                        for (String uid : uids) {
                                            WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                            warns.add(warn);
                                        }
                                    }
                                    tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtdura) + dura));
                                }
                            }
                        }
                }
            }
        }

    }

    public static void executeEX_TH_0_4_1(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

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
                                Long hour = Long.valueOf(hd.get("time").toString().split(" ")[1].split(":")[0]);
                                if (0 < hour && hour < 4) {
                                    logger.info("话单记录-手机号码相同:" + hd.get("phone"));
                                    Long dura = Long.valueOf(hd.get("dura").toString());
                                    String mtdura = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId());
                                    if (mtdura == null) mtdura = "0";
                                    if (Long.valueOf(mtdura) + dura > 3600) {
                                        String msg = hd.get("phone") + "当天0-4点拨打电话通话总时间超过1小时";
                                        for (String uid : uids) {
                                            WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                            warns.add(warn);
                                        }
                                    }
                                    tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtdura) + dura));
                                }
                            }
                        }
                }
            }
        }

    }

    public static void executeEX_THCS_0_4_3FC(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

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
                                Long hour = Long.valueOf(hd.get("time").toString().split(" ")[1].split(":")[0]);
                                if (0 < hour && hour < 4) {
                                    logger.info("话单记录-手机号码相同:" + hd.get("phone"));
                                    String mtdura = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId());
                                    if (mtdura == null) mtdura = "0";
                                    if (Long.valueOf(mtdura) + 1 - HbaseService.getAvgThCs0_4(hd.get("phone").toString()) > 3 * HbaseService.getStdThCs0_4(hd.get("phone").toString())) {
                                        String msg = hd.get("phone") + "当天0-4点拨打电话次数超过过去一个月3倍方差";
                                        for (String uid : uids) {
                                            WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                            warns.add(warn);
                                        }
                                    }
                                    tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtdura) + 1));
                                }
                            }
                        }
                }
            }
        }

    }

    public static void executeEX_THCS_5(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

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
                                String mtdura = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId());
                                if (mtdura == null) mtdura = "0";
                                if (Long.valueOf(mtdura) + 1 > 5) {
                                    String msg = hd.get("phone") + "当天拨打电话次数超过5次";
                                    for (String uid : uids) {
                                        WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                        warns.add(warn);
                                    }
                                }
                                tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtdura) + 1));
                            }
                        }
                }
            }
        }

    }

    public static void executeEX_TH_3FC(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

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
                                Long dura = Long.valueOf(hd.get("dura").toString());
                                String mtdura = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId());
                                if (mtdura == null) mtdura = "0";
                                if (Long.valueOf(mtdura) + dura - HbaseService.getAvgThSc(hd.get("phone").toString()) > 3 * HbaseService.getStdThSc(hd.get("phone").toString())) {
                                    String msg = hd.get("phone") + "当天拨打电话通话总时间超过过去一个月3倍方差";
                                    for (String uid : uids) {
                                        WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                        warns.add(warn);
                                    }
                                }
                                tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtdura) + dura));
                            }
                        }
                }
            }
        }

    }

    public static void executeEX_THCS_3FC(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

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
                                String mtdura = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId());
                                if (mtdura == null) mtdura = "0";
                                if (Long.valueOf(mtdura) + 1 - HbaseService.getAvgThCs(hd.get("phone").toString()) > 3 * HbaseService.getStdThCs(hd.get("phone").toString())) {
                                    String msg = hd.get("phone") + "当天拨打电话次数超过过去一个月3倍方差";
                                    for (String uid : uids) {
                                        WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                        warns.add(warn);
                                    }
                                }
                                tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtdura) + 1));
                            }
                        }
                }
            }
        }

    }

    public static void executeExTh10(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

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
                                Long hour = Long.valueOf(hd.get("time").toString().split(" ")[1].split(":")[0]);
                                if (0 < hour && hour < 4) {
                                    logger.info("话单记录-手机号码相同:" + hd.get("phone"));
                                    String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId());
                                    if (mtthcs == null) mtthcs = "0";
                                    if (Long.valueOf(mtthcs) + 1 > 10) {
                                        String msg = hd.get("phone") + " 0-4点通话次数超过10次";
                                        for (String uid : uids) {
                                            WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
                                            warns.add(warn);
                                        }
                                    }
                                    tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtthcs) + 1));
                                }
                            }
                        }
                }
            }
        }

    }

    public static void executeEX_TH_5(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

        //特定电话号码列表
        if (rule.getPhone_number() != null && rule.getPhone_number().length() > 0) {
            List<String> phoneList = Arrays.asList(rule.getPhone_number().split(","));
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
                                if (sm.getData_value().equals(hd.get("phone")) && phoneList.contains(hd.get("tarnum"))) {
                                    Long hour = Long.valueOf(hd.get("time").toString().split(" ")[1].split(":")[0]);
                                    if (0 < hour && hour < 4) {
                                        logger.info("话单记录-手机号码相同:" + hd.get("phone"));
                                        String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId() + "@@" + hd.get("tarnum"));
                                        if (mtthcs == null) mtthcs = "0";
                                        if (Long.valueOf(mtthcs) + 1 > 5) {
                                            String msg = hd.get("phone") + " 0-4点与" + hd.get("tarnum") + "通话次数超过5次";
                                            for (String uid : uids) {
                                                WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
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
        }

    }

    public static void executeEX_TH_0_4_TD3FC(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

        //特定电话号码列表
        if (rule.getPhone_number() != null && rule.getPhone_number().length() > 0) {
            List<String> phoneList = Arrays.asList(rule.getPhone_number().split(","));
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
                                if (sm.getData_value().equals(hd.get("phone")) && phoneList.contains(hd.get("tarnum"))) {
                                    Long hour = Long.valueOf(hd.get("time").toString().split(" ")[1].split(":")[0]);
                                    if (0 < hour && hour < 4) {
                                        logger.info("话单记录-手机号码相同:" + hd.get("phone"));
                                        Long dura = Long.valueOf(hd.get("dura").toString());
                                        String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId() + "@@" + hd.get("tarnum"));
                                        if (mtthcs == null) mtthcs = "0";
                                        if (Long.valueOf(mtthcs) + dura - HbaseService.getAvgThsc0_4(hd.get("phone").toString(), hd.get("tarnum").toString()) > HbaseService.getStdThsc0_4(hd.get("phone").toString(), hd.get("tarnum").toString())) {
                                            String msg = hd.get("phone") + " 0-4点与" + hd.get("tarnum") + "通话时长超过过去一个月的3倍";
                                            for (String uid : uids) {
                                                WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
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
        }

    }

    public static void executeEX_TH_0_4_TD_1(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

        //特定电话号码列表
        if (rule.getPhone_number() != null && rule.getPhone_number().length() > 0) {
            List<String> phoneList = Arrays.asList(rule.getPhone_number().split(","));
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
                                if (sm.getData_value().equals(hd.get("phone")) && phoneList.contains(hd.get("tarnum"))) {
                                    Long hour = Long.valueOf(hd.get("time").toString().split(" ")[1].split(":")[0]);
                                    if (0 < hour && hour < 4) {
                                        logger.info("话单记录-手机号码相同:" + hd.get("phone"));
                                        Long dura = Long.valueOf(hd.get("dura").toString());
                                        String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId() + "@@" + hd.get("tarnum"));
                                        if (mtthcs == null) mtthcs = "0";
                                        if (Long.valueOf(mtthcs) + dura > 3600) {
                                            String msg = hd.get("phone") + " 0-4点与" + hd.get("tarnum") + "通话时长超过1h";
                                            for (String uid : uids) {
                                                WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
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
        }

    }

    public static void executeEX_THCS_0_4_TD3FC(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

        //特定电话号码列表
        if (rule.getPhone_number() != null && rule.getPhone_number().length() > 0) {
            List<String> phoneList = Arrays.asList(rule.getPhone_number().split(","));
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
                                if (sm.getData_value().equals(hd.get("phone")) && phoneList.contains(hd.get("tarnum"))) {
                                    Long hour = Long.valueOf(hd.get("time").toString().split(" ")[1].split(":")[0]);
                                    if (0 < hour && hour < 4) {
                                        logger.info("话单记录-手机号码相同:" + hd.get("phone"));
                                        String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0] + "@@" + rule.getId() + "@@" + hd.get("tarnum"));
                                        if (mtthcs == null) mtthcs = "0";
                                        if (Long.valueOf(mtthcs) + 1 - HbaseService.getAvgThsc0_4(hd.get("phone").toString(), hd.get("tarnum").toString()) > HbaseService.getStdThsc0_4(hd.get("phone").toString(), hd.get("tarnum").toString())) {
                                            String msg = hd.get("phone") + " 0-4点与" + hd.get("tarnum") + "通话次数超过过去一个月的3倍";
                                            for (String uid : uids) {
                                                WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_HD, uid);
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
        }

    }

    public static void executeEX_KD_3FC(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

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
                        //寄递数据中有电话号码
                        if (message.getType() == Msg.WJ_MD) {
                            Map hd = message.getContext();
                            if (sm.getData_value().equals(hd.get("phone"))) {
                                logger.info("寄递记录-手机号码相同:" + hd.get("phone"));
                                String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId());
                                if (mtthcs == null) mtthcs = "0";
                                if (Long.valueOf(mtthcs) + 1 - HbaseService.getAvgJdCs(hd.get("phone").toString()) > 3 * HbaseService.getStdJdCs(hd.get("phone").toString())) {
                                    String msg = hd.get("phone") + " 平均每月寄出快递数据超过15次";
                                    for (String uid : uids) {
                                        WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_JD, uid);
                                        warns.add(warn);
                                    }
                                }
                                tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtthcs) + 1));
                            }
                        }
                }
            }
        }

    }

    public static void executeEX_KD_15(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

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
                        //寄递数据中有电话号码
                        if (message.getType() == Msg.WJ_MD) {
                            Map hd = message.getContext();
                            if (sm.getData_value().equals(hd.get("phone"))) {
                                logger.info("寄递记录-手机号码相同:" + hd.get("phone"));
                                String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId());
                                if (mtthcs == null) mtthcs = "0";
                                if (Long.valueOf(mtthcs) + 1 > 15 && HbaseService.getAvgJdCs(hd.get("phone").toString()) > 15) {
                                    String msg = hd.get("phone") + " 平均每月寄出快递数据超过15次";
                                    for (String uid : uids) {
                                        WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_JD, uid);
                                        warns.add(warn);
                                    }
                                }
                                tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtthcs) + 1));
                            }
                        }
                }
            }
        }

    }

    public static void executeEX_KD_30(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

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
                        //寄递数据中有电话号码
                        if (message.getType() == Msg.WJ_MD) {
                            Map hd = message.getContext();
                            if (sm.getData_value().equals(hd.get("phone"))) {
                                logger.info("寄递记录-手机号码相同:" + hd.get("phone"));
                                String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId());
                                if (mtthcs == null) mtthcs = "0";
                                if (Long.valueOf(mtthcs) + 1 > 30) {
                                    String msg = hd.get("phone") + " 当月寄出快递数据超过30次";
                                    for (String uid : uids) {
                                        WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_JD, uid);
                                        warns.add(warn);
                                    }
                                }
                                tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtthcs) + 1));
                            }
                        }
                }
            }
        }

    }

    public static void executeEX_WB_20(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

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
                        //寄递数据中有电话号码
                        if (message.getType() == Msg.wj_net_use) {
                            Map hd = message.getContext();
                            if (sm.getData_value().equals(hd.get("phone"))) {
                                logger.info("网吧记录-手机号码相同:" + hd.get("phone"));
                                String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId());
                                if (mtthcs == null) mtthcs = "0";
                                if (Long.valueOf(mtthcs) + 1 > 20) {
                                    String msg = hd.get("phone") + " 当月去网吧超过20次";
                                    for (String uid : uids) {
                                        WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_JD, uid);
                                        warns.add(warn);
                                    }
                                }
                                tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtthcs) + 1));
                            }
                        }
                }
            }
        }

    }

    public static void executeEX_WB_15(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

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
                        //寄递数据中有电话号码
                        if (message.getType() == Msg.wj_net_use) {
                            Map hd = message.getContext();
                            if (sm.getData_value().equals(hd.get("phone"))) {
                                logger.info("网吧记录-手机号码相同:" + hd.get("phone"));
                                String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId());
                                if (mtthcs == null) mtthcs = "0";
                                if (Long.valueOf(mtthcs) + 1 > 15 && HbaseService.getAvgWbCs(hd.get("phone").toString()) > 15) {
                                    String msg = hd.get("phone") + " 当月去网吧超过20次";
                                    for (String uid : uids) {
                                        WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_JD, uid);
                                        warns.add(warn);
                                    }
                                }
                                tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtthcs) + 1));
                            }
                        }
                }
            }
        }

    }

    public static void executeEX_WB_3FC(Msg message, List<SupectModel> supects, List<KeyAreaModel> areas, WarningRuleModel rule, List<WarningModel> warns, Map<String, String> tempCache) {

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
                        //寄递数据中有电话号码
                        if (message.getType() == Msg.wj_net_use) {
                            Map hd = message.getContext();
                            if (sm.getData_value().equals(hd.get("phone"))) {
                                logger.info("网吧记录-手机号码相同:" + hd.get("phone"));
                                String mtthcs = RedisService.getInstance().get(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId());
                                if (mtthcs == null) mtthcs = "0";
                                if (Long.valueOf(mtthcs) + 1 - HbaseService.getAvgWbCs(hd.get("phone").toString()) > 3 * HbaseService.getStdWbCs(hd.get("phone").toString())) {
                                    String msg = hd.get("phone") + " 当月去网吧次数与均值的差超过过去一个月的3倍标准差";
                                    for (String uid : uids) {
                                        WarningModel warn = RuleCompareService.generate(sm.getBus_id(), hd.get("time").toString(), msg, rule.getId(), WarningModel.SOURCE_TYPE_JD, uid);
                                        warns.add(warn);
                                    }
                                }
                                tempCache.put(hd.get("phone").toString() + "@@" + hd.get("time").toString().split(" ")[0].substring(0, 7) + "@@" + rule.getId(), String.valueOf(Long.valueOf(mtthcs) + 1));
                            }
                        }
                }
            }
        }

    }
}
