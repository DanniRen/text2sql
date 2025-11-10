# Excel文件转换结果
转换时间：2025-11-06 22:56:58
转换文件数量：10

## 📊 乐园 UGC 内容体系

| 表名 | 外键字段 | 关联表 | 关联字段 | 业务说明 |
| --- | --- | --- | --- | --- |
| dws_jordass_matchlog_stat_di | vplayerid | dws_jordass_login_df | vplayerid | 乐园玩法表映射到玩家登录表 |
| dws_jordass_matchlog_stat_di | imode | dim_jordass_submodeonline_nf | matchsubmodegroup | 乐园玩法映射到子玩法上线表 |
| dws_jordass_matchlog_stat_di | imode | dim_jordass_imode_leyuan_nf | imode | 乐园玩法映射到上线周期表 |
| dws_jordass_playermatchrecord_stat_df | vplayerid | dws_jordass_login_df | vplayerid | 乐园模式位图表映射到登录表 |
| dws_jordass_playermatchrecord_stat_df | imode | dim_jordass_submodeonline_nf | matchsubmodegroup | 乐园模式映射到子玩法上线表 |
| dim_jordass_leyuan_participate_cdf_nf | vplayerid | dws_jordass_login_df | vplayerid | 乐园参与记录映射到玩家登录 |
| dim_jordass_leyuan_participate_cdf_nf | matchsubmodegroup | dim_jordass_submodeonline_nf | matchsubmodegroup | 乐园参与记录映射到子玩法 |
| dwd_jordass_activitypersonaldata_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 活动个人数据映射到角色登录表 |
| dwd_jordass_activitypersonaldata_hi | battleid | dwd_jordass_gameresultrecord_hi | battleid | 活动数据映射到乐园对局结果 |

*数据来源：乐园 UGC 内容体系.xlsx*

---

## 📊 玩家基础行为与活跃

| 表名 | 外键字段 | 关联表 | 关联字段 | 业务说明 |
| --- | --- | --- | --- | --- |
| dws_argothek_ce1_login_di | vplayerid, vgameappid, platid | dim_vplayerid_vies_df | vplayerid, vgameappid, platid | 登录行为映射到玩家全量信息 |
| dws_jordass_login_df | vplayerid, vgameappid, platid | dim_vplayerid_vies_df | vplayerid, vgameappid, platid | 砺刃玩家登录数据映射到全量信息 |
| dws_jordass_login_di | vplayerid, vgameappid, platid | dws_jordass_login_df | vplayerid, vgameappid, platid | 日粒度登录行为汇总映射到位图表 |
| dwd_jordass_playerlogin_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 玩家登录流水映射到角色位图表 |
| dwd_jordass_playerlogout_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 玩家登出流水映射到角色位图表 |
| dws_jordass_device_login_di | vplayerid | dws_jordass_login_di | vplayerid | 多端登录行为映射到玩家登录汇总 |
| dwd_jordass_playerexitgamerecord_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 退出对局行为映射到角色登录表 |
| dws_argothek_ce1_cbt2_vplayerid_suserid_di | vplayerid | dws_argothek_ce1_login_di | vplayerid | CBT2标签映射到登录玩家 |
| dws_jordass_emulator_df | vplayerid | dws_jordass_login_df | vplayerid | 模拟器用户映射到登录玩家 |

*数据来源：玩家基础行为与活跃.xlsx*

---

## 📊 社交与联盟系统

| 表名 | 外键字段 | 关联表 | 关联字段 | 业务说明 |
| --- | --- | --- | --- | --- |
| dwd_jordass_friendlist_hi | vplayerid, uid | dwd_jordass_playerlogin_hi | vplayerid, uid | 好友列表映射到登录流水 |
| dwd_jordass_friendlist_hi | vplayerid | dws_jordass_login_df | vplayerid | 好友列表映射到登录汇总 |
| dwd_jordass_allianceshopbuy_hi | vplayerid, uid | dwd_jordass_playerlogin_hi | vplayerid, uid | 联盟商店购买映射到登录流水 |
| dwd_jordass_alliancebootcamprecord_hi | memberuid | dws_jordass_uid_login_df | uid | 联盟特训记录映射到角色ID |
| dwd_jordass_alliancebootcamprecord_hi | corpsid | dwd_jordass_player_allianceactive_hi | corpsid | 联盟ID映射到联盟活跃点表 |
| dwd_jordass_player_allianceactive_hi | uid | dws_jordass_uid_login_df | uid | 联盟活跃点映射到角色ID |
| dwd_jordass_msgchatrecord_hi | playerid, roleid | dws_jordass_uid_login_df | vplayerid, uid | 聊天记录映射到角色登录表 |
| dwd_jordass_msgchatrecord_hi | receiverplayerid, receiverroleid | dws_jordass_uid_login_df | vplayerid, uid | 消息接收方映射到角色登录表 |
| dwd_jordass_vsteamshoplog_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 组竞商店购买映射到角色登录表 |

*数据来源：社交与联盟系统.xlsx*

---

## 📊 玩家画像与标签体系

| 表名 | 外键字段 | 关联表 | 关联字段 | 业务说明 |
| --- | --- | --- | --- | --- |
| dws_jordass_useralltag_df | playerid | dws_jordass_login_df | vplayerid | 玩家标签映射到登录表 |
| dws_jordass_useralltag_df | playerid | dim_vplayerid_vies_df | vplayerid | 玩家标签映射到全量玩家表 |
| dim_uf_player_gameinfo_mf | userid | dim_argothek_gplayerid2qqwxid_df | suserid | 人口学数据映射到ID转换表 |
| dws_jordass_player_allstate_df | vplayerid, vgameappid, platid | dws_jordass_login_df | vplayerid, vgameappid, platid | 玩家状态映射到登录表 |
| dws_jordass_player_allstate_df | vplayerid | dws_jordass_useralltag_df | playerid | 玩家状态映射到用户标签表 |
| dws_jordass_player_allstate_df | vplayerid | dws_jordass_water_df | vplayerid | 玩家状态映射到流水位图表 |

*数据来源：玩家画像与标签体系.xlsx*

---

## 📊 游戏对局与行为数据

| 表名 | 外键字段 | 关联表 | 关联字段 | 业务说明 |
| --- | --- | --- | --- | --- |
| dwd_jordass_player_matchrecord_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 玩家匹配记录映射到角色登录表 |
| dwd_jordass_player_matchrecord_hi | matchsubmodegroup | dim_jordass_submodeonline_nf | matchsubmodegroup | 匹配模式映射到子玩法上线表 |
| dwd_jordass_roundflow_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 传统模式对局映射到角色登录表 |
| dwd_jordass_roundflow_hi | vplayerid | dws_jordass_login_di | vplayerid | 对局数据映射到登录行为表 |
| dwd_jordass_gameresultrecord_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 乐园创作间对局映射到角色登录表 |
| dwd_jordass_vsteam_roundrecord_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 组竞模式对局映射到角色登录表 |
| dwd_jordass_roundflow_entertain_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 微赛事对局记录映射到角色登录表 |
| dwd_jordass_roundlog_funnymode_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 休闲模式对局映射到角色登录表 |
| dwd_jordass_vsteaminfectlog_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 生化模式对局映射到角色登录表 |
| dwd_jordass_wereplayerdatallianceforcelow_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 狼人杀模式对局映射到角色登录表 |
| dwd_jordass_player_matchrecord_hi | gameid | dwd_jordass_roundflow_hi | battleid | 匹配记录映射到对局结果 |
| dwd_jordass_player_matchrecord_hi | gameid | dwd_jordass_gameresultrecord_hi | battleid | 匹配记录映射到乐园对局结果 |
| dwd_jordass_player_matchrecord_hi | gameid | dwd_jordass_vsteam_roundrecord_hi | battleid | 匹配记录映射到组竞对局结果 |

*数据来源：游戏对局与行为数据.xlsx*

---

## 📊 按钮行为及交互系统

| 表名 | 外键字段 | 关联表 | 关联字段 | 业务说明 |
| --- | --- | --- | --- | --- |
| dwd_jordass_pressbutton_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 按钮点击映射到角色登录表 |
| dwd_jordass_activitypress_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 活动按钮点击映射到角色登录表 |
| dws_jordass_buttonpress_pre_di | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 按钮点击行为汇总映射到角色表 |
| dwd_jordass_voicethemelog_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 语音主题解锁映射到角色登录表 |

*数据来源：按钮行为及交互系统.xlsx*

---

## 📊 Argothek(瓦罗兰特)游戏体系

| 表名 | 外键字段 | 关联表 | 关联字段 | 业务说明 |
| --- | --- | --- | --- | --- |
| dwd_argothek_playermatchstats_hi | vroleid | dim_argothek_gplayerid_vroleid_df | vRoleID | 玩家对局表现映射到角色ID转换表 |
| dwd_argothek_playermatchstats_hi | matchid | dwd_argothek_matchdetails_hi | matchid | 玩家表现映射到比赛详情 |
| dwd_argothek_matchdetails_hi | matchid | dwd_argothek_playerloadout_hi | matchinfomatchid | 比赛详情映射到玩家装备信息 |
| dwd_argothek_playerloadout_hi | subject | dim_argothek_gplayerid2qqwxid_df | subject | 装备信息映射到玩家ID转换表 |
| dwd_argothek_commercialization_gearid_df | purchasedItemId | dwd_argothek_apinventorymovementlog_hi | itemid | 商业化道具映射到物品变动日志 |
| dwd_argothek_playerlogout_hi | iuserid | dim_argothek_gplayerid2qqwxid_df | iuserid | 玩家登出映射到ID转换表 |
| dwd_argothek_playerlogin_hi | iuserid | dim_argothek_gplayerid2qqwxid_df | iuserid | 玩家登录映射到ID转换表 |
| dwd_argothek_gearrecord_df | iuserid | dim_argothek_gplayerid2qqwxid_df | iuserid | 道具记录映射到ID转换表 |

*数据来源：Argothek(瓦罗兰特)游戏体系.xlsx*

---

## 📊 玩家身份与映射体系

| 表名 | 外键字段 | 关联表 | 关联字段 | 业务说明 |
| --- | --- | --- | --- | --- |
| dim_mgamejp_tbplayerid2wxid_nf | splayerid | dws_argothek_ce1_login_di | vplayerid | playerid映射到登录玩家 |
| dim_mgamejp_tbplayerid2wxid_nf | splayerid | dim_vplayerid_vies_df | vplayerid | 玩家ID映射到重合竞品表 |
| dim_mgamejp_tbplayerid2qq_nf | splayerid | dws_argothek_ce1_login_di | vplayerid | playerid映射到登录玩家 |
| dim_mgamejp_idconversion_wxid_qq_nf | swxid | dim_mgamejp_tbplayerid2wxid_nf | swxid | 微信ID映射到playerid体系 |
| dim_mgamejp_idconversion_wxid_qq_nf | iqq | dim_mgamejp_tbplayerid2qq_nf | iqq | QQ号映射到playerid体系 |
| dim_argothek_gplayerid2qqwxid_df | vplayerid | dws_argothek_ce1_login_di | vplayerid | gplayerid映射到登录玩家 |
| dim_argothek_gplayerid2qqwxid_df | suserid | dim_uf_player_gameinfo_mf | userid | 账号映射到人口学信息表 |
| dim_jordass_playerid2suserid_nf | vplayerid | dws_jordass_login_df | vplayerid | playerid映射到砺刃登录表 |
| dim_argothek_gplayerid_vroleid_df | iuserid | dws_argothek_ce1_login_di | vplayerid | 角色ID映射到playerid |
| dim_argothek_gplayerid_vroleid_df | vRoleID | dws_jordass_uid_login_df | uid | 角色ID映射到角色登录表 |

*数据来源：玩家身份与映射体系.xlsx*

---

## 📊 赛季与活动周期

| 表名 | 外键字段 | 关联表 | 关联字段 | 业务说明 |
| --- | --- | --- | --- | --- |
| dim_argothek_seasondate_df | seasonid | dwd_argothek_matchdetails_hi | seasonid | 赛季时间映射到比赛详情 |
| dim_jordass_submodeonlinedate_conf | matchsubmodegroup | dim_jordass_submodeonline_nf | matchsubmodegroup | 子玩法上线日期配置映射到数据表 |
| dim_jordass_imode_leyuan_nf | imode | dws_jordass_matchlog_stat_di | imode | 乐园玩法周期映射到玩法模式表 |
| dwd_jordass_playerprofstagechange_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 专业赛赛段变更映射到角色登录表 |

*数据来源：赛季与活动周期.xlsx*

---

## 📊 商业化与付费体系

| 表名 | 外键字段 | 关联表 | 关联字段 | 业务说明 |
| --- | --- | --- | --- | --- |
| dws_jordass_water_df | vplayerid, vgameappid, platid | dws_jordass_login_df | vplayerid, vgameappid, platid | 流水位图表映射到玩家登录表 |
| dws_jordass_water_di | vplayerid, vgameappid, platid | dws_jordass_login_df | vplayerid, vgameappid, platid | 流水事实表映射到登录汇总 |
| dwd_jordass_payrespond_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 付费响应映射到角色登录表 |
| dwd_jordass_marketpurchase_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 商城购买映射到角色登录表 |
| dwd_jordass_lotteryrecord_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 轮盘抽奖映射到角色登录表 |
| dwd_jordass_directpaycommflow_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 直购流水映射到角色登录表 |
| dwd_jordass_currencylog_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 货币流水映射到角色登录表 |
| dwd_jordass_currencylog_hi | vplayerid | dwd_jordass_payrespond_hi | vplayerid | 货币流水映射到付费响应 |
| dwd_jordass_privpackagelog_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 特权补给包映射到角色登录表 |
| dwd_jordass_gearlog_hi | vplayerid, uid | dws_jordass_uid_login_df | vplayerid, uid | 道具流水映射到角色登录表 |

*数据来源：商业化与付费体系.xlsx*

---

