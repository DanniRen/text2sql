#!/usr/bin/env python3
import pandas as pd
import csv

def map_table_names():
    """将 column_names_original.csv 文件里的 table_index 和表名对应起来，生成新的 CSV 文件"""
    
    # 表名列表，按顺序与 table_index 对应
    table_names = [
        "dim_argothek_gplayerid2qqwxid_df",
        "dim_argothek_gplayerid_vroleid_df", 
        "dim_argothek_seasondate_df",
        "dim_extract_311381_conf",
        "dim_jordass_imode_leyuan_nf",
        "dim_jordass_leyuan_participate_cdf_nf",
        "dim_jordass_packet_conf",
        "dim_jordass_playerid2suserid_nf",
        "dim_jordass_submodeonline_nf",
        "dim_jordass_submodeonlinedate_conf",
        "dim_mgamejp_account_allinfo_nf",
        "dim_mgamejp_idconversion_wxid_qq_nf",
        "dim_mgamejp_tbplayerid2qq_nf",
        "dim_mgamejp_tbplayerid2wxid_nf",
        "dim_uf_player_gameinfo_mf",
        "dim_vplayerid_vies_df",
        "dwd_argothek_abilityinfo_effects_hi",
        "dwd_argothek_apinventorymovementlog_hi",
        "dwd_argothek_cbt2kaibai_hi",
        "dwd_argothek_commercialization_gearid_df",
        "dwd_argothek_gameclientrecord_hi",
        "dwd_argothek_gearrecord_df",
        "dwd_argothek_matchdetails_hi",
        "dwd_argothek_playerloadout_expressions_hi",
        "dwd_argothek_playerloadout_hi",
        "dwd_argothek_playerloadout_sprayslots_hi",
        "dwd_argothek_playerlogin_hi",
        "dwd_argothek_playerlogout_hi",
        "dwd_argothek_playermatchdetail_hi",
        "dwd_argothek_playermatchstats_hi",
        "dwd_jordass_activity_rewardrecord_hi",
        "dwd_jordass_activitypersonaldata_hi",
        "dwd_jordass_activitypress_hi",
        "dwd_jordass_activityrecord_hi",
        "dwd_jordass_alliancebootcamprecord_hi",
        "dwd_jordass_allianceshopbuy_hi",
        "dwd_jordass_buttonpress_pre_di",
        "dwd_jordass_currencylog_hi",
        "dwd_jordass_directpaycommflow_hi",
        "dwd_jordass_friendlist_hi",
        "dwd_jordass_gameresultrecord_hi",
        "dwd_jordass_gearlog_hi",
        "dwd_jordass_lotterylog_twostage_hi",
        "dwd_jordass_lotteryrecord_hi",
        "dwd_jordass_marketpurchase_hi",
        "dwd_jordass_msgchatrecord_hi",
        "dwd_jordass_mysteryplaylog_hi",
        "dwd_jordass_payrespond_hi",
        "dwd_jordass_player_allianceactive_hi",
        "dwd_jordass_player_matchrecord_hi",
        "dwd_jordass_playerexitgamerecord_hi",
        "dwd_jordass_playerlogin_hi",
        "dwd_jordass_playerlogout_hi",
        "dwd_jordass_playerprofstagechange_hi",
        "dwd_jordass_pressbutton_hi",
        "dwd_jordass_privpackagelog_hi",
        "dwd_jordass_roundflow_entertain_hi",
        "dwd_jordass_roundflow_hi",
        "dwd_jordass_roundlog_funnymode_hi",
        "dwd_jordass_voicethemelog_hi",
        "dwd_jordass_vsteam_roundrecord_hi",
        "dwd_jordass_vsteaminfectlog_hi",
        "dwd_jordass_vsteamshoplog_hi",
        "dwd_jordass_wereplayerdatallianceforcelow_hi",
        "dws_argothek_ce1_cbt2_vplayerid_suserid_di",
        "dws_argothek_ce1_login_di",
        "dws_argothek_oss_login_di",
        "dws_argothek_oss_portaltransaction_di",
        "dws_argothek_oss_useractivity_df",
        "dws_jordass_buttonpress_pre_di",
        "dws_jordass_device_login_di",
        "dws_jordass_emulator_df",
        "dws_jordass_login_df",
        "dws_jordass_login_di",
        "dws_jordass_matchlog_stat_di",
        "dws_jordass_mode_roundrecord_di",
        "dws_jordass_player_allstate_df",
        "dws_jordass_playermatchrecord_stat_df",
        "dws_jordass_uid_login_df",
        "dws_jordass_useralltag_df",
        "dws_jordass_water_df",
        "dws_jordass_water_di",
        "dws_mgamejp_login_user_activity_di",
        "dws_pcgame_dayact_di"
    ]
    
    # 读取原始 CSV 文件
    input_file = '../../data/column_names_original.csv'
    output_file = '../../data/column_names_with_table_names.csv'
    
    # 读取数据
    df = pd.read_csv(input_file)
    
    # 创建表名映射字典
    table_name_map = {}
    for i, table_name in enumerate(table_names):
        table_name_map[i] = table_name
    
    # 添加表名列
    df['table_name'] = df['table_index'].map(table_name_map)
    
    # 对于 table_index = -1 的行，表名设为 'Unknown'
    df.loc[df['table_index'] == -1, 'table_name'] = 'Unknown'
    
    # 重新排列列的顺序：table_index, table_name, column_index, column_name
    df = df[['table_index', 'table_name', 'column_index', 'column_name']]
    
    # 保存到新的 CSV 文件
    df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"处理完成！生成了新文件: {output_file}")
    print(f"总共处理了 {len(df)} 行数据")
    print(f"识别到 {len(table_names)} 个表名")
    
    # 显示一些统计信息
    print("\n统计信息:")
    print(f"- 表索引范围: {df['table_index'].min()} 到 {df['table_index'].max()}")
    print(f"- 未知表名行数: {len(df[df['table_name'] == 'Unknown'])}")
    print(f"- 有表名行数: {len(df[df['table_name'] != 'Unknown'])}")
    
    # 显示前几行作为示例
    print("\n前10行数据预览:")
    print(df.head(10))

if __name__ == "__main__":
    map_table_names()