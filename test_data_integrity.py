import os
from tqdm.auto import tqdm
import numpy as np
import pandas as pd
import datetime as dt
from workflow.vulcan import HiveClient
from workflow.settings import HIVE_PERFORMANCE_SETTINGS
from IPython.display import display


# configure everytime
# TILL_CHN_TIME = dt.datetime.fromisoformat("2023-10-19 14:00:00")
TILL_MEX_TIME = dt.datetime.fromisoformat("2023-10-17 00:55:10")
TILL_MEX_TIME = dt.datetime.fromisoformat("2023-10-20 20:55:10")
N_FEATURES = 434
VAR_GROUP = "plist_variable"
FEATURE_PREFIX = "plist_"

# configure scacely
DIR_WORK_DATA = "./work_data/"
# TILL_MEX_TIME = TILL_CHN_TIME - dt.timedelta(hours=14)
TILL_CHN_TIME = TILL_MEX_TIME + dt.timedelta(hours=14)
ONLINE_FILE = os.path.join(
    DIR_WORK_DATA,
    "{}online_{}.csv".format(FEATURE_PREFIX, TILL_MEX_TIME.strftime("%Y-%m-%d"))
)
OffLINE_FILE = os.path.join(
    DIR_WORK_DATA,
    "{}offline_{}.csv".format(FEATURE_PREFIX, TILL_MEX_TIME.strftime("%Y-%m-%d"))
)
ONLINE_VAR_FILE = os.path.join(
    DIR_WORK_DATA,
    "{}online_var_{}.csv".format(FEATURE_PREFIX, TILL_MEX_TIME.strftime("%Y-%m-%d"))
)
OFFLINE_VAR_FILE = os.path.join(
    DIR_WORK_DATA,
    "{}offline_var_{}.csv".format(FEATURE_PREFIX, TILL_MEX_TIME.strftime("%Y-%m-%d"))
)
PCT_MATCH_VAR_FILE = os.path.join(
    DIR_WORK_DATA,
    "{}pct_match_var_{}.csv".format(FEATURE_PREFIX, TILL_MEX_TIME.strftime("%Y-%m-%d"))
)
NUM_IS_CLOSE = 1e-4
ACCEPT_RATE = 5e-3

# hiveql: support regex in column identifiers
hive_config = HIVE_PERFORMANCE_SETTINGS.copy()
hive_config["hive.support.quoted.identifiers"] = "none"

hive = HiveClient("mex", config=hive_config)

with open("../plist/Sql/Step1_DataCleaning.sql") as f:
    lst_sqls = f.read().split(';')

for sql in tqdm(lst_sqls):
    if len(sql.strip()) > 0:
        hive.execute(sql)

with open("../plist/Sql/Step2_AtoLabeling.sql") as f:
    lst_sqls = f.read().split(';')

for sql in tqdm(lst_sqls):
    if len(sql.strip()) > 0:
        hive.execute(sql)

with open("../plist/Sql/Step3-0_Agg.sql") as f:
    lst_sqls = f.read().split(';')

for sql in tqdm(lst_sqls):
    if len(sql.strip()) > 0:
        hive.execute(sql)

with open("../plist/Sql/Step3-1_Agg.sql") as f:
    lst_sqls = f.read().split(';')

for sql in tqdm(lst_sqls[6:]):
    if len(sql.strip()) > 0:
        hive.execute(sql, {"hive.execution.engine": "mr" if sql.lower().count("union all") > 3 else "tez"})

with open("../plist/Sql/Step3-2_Agg.sql") as f:
    lst_sqls = f.read().split(';')

for sql in tqdm(lst_sqls):
    if len(sql.strip()) > 0:
        hive.execute(sql)

with open("../plist/Sql/Step4-1_Derivant.sql") as f:
    lst_sqls = f.read().split(';')

for sql in tqdm(lst_sqls):
    if len(sql.strip()) > 0:
        hive.execute(sql)

# online plist table
hive.execute(
f"""--hive
--create table tmp.lizhonghao_online_plist_check as
insert overwrite table tmp.lizhonghao_online_plist_check
select user_key
    ,verify_key
    ,key
    ,value
    ,dt
from gdm.ris_var_info_inc
where var_group = '{VAR_GROUP}'
and dt >= '{TILL_MEX_TIME.strftime("%Y-%m-%d")}'
and rsk_step = 'A'
--!hive
"""
)
df_online = hive.run_hql("select * from tmp.lizhonghao_online_plist_check")
df_online.to_csv(ONLINE_FILE, index=False)

# offline plist table
hive.execute(
f"""--hive
--create table tmp.lizhonghao_offline_plist_check as
with A_step_users as (
    select loan_key
        ,user_key
        ,create_time
        ,dt
    from fdm.mongo_strategyinvoke_inc
    where step = 'A'
        and dt >= '{TILL_MEX_TIME.strftime("%Y-%m-%d")}'
),
features as (
    select a.user_key
        ,A_step_users.loan_key
        ,A_step_users.dt
        ,row_number() over (partition by a.user_key order by a.http_timestamp desc) as f
        ,a.http_timestamp
        ,plist_pn_shopping_ut7d_180d_ratio
        ,plist_pn_travelandlocal_ut180d_ratio
        ,plist_pn_cashloan_rating45_ut7d_180d_ratio
        ,plist_pn_business_ut7d_ratio
        ,plist_pn_xiaomi_ut15d_all_ratio
        ,plist_pn_t20_travelandlocal_ratio
        ,plist_pn_communication_ut15d_all_ratio
        ,plist_pn_frt100_szqb_ratio
        ,plist_pn_ncategory_ratio
        ,plist_pn_frt200_ratio
        ,plist_pn_cashloan_rating45_ut7d_90d_ratio
        ,plist_pn_t50_videoplayers_cnt
        ,plist_pn_social_ut30d_ratio
        ,plist_pn_bank_ratio
        ,plist_pn_rloan_ut7d_7d_all_ratio
        ,plist_pn_cashloan_rating3_cnt
        ,plist_pn_samsung_ut90d_180d_ratio
        ,plist_pn_has_subscriptions
        ,plist_pn_loan_ut7d_ratio
        ,plist_pn_finservice_ut15d_180d_ratio
        ,plist_pn_cashloan_ut1d_180d_all_ratio
        ,plist_pn_autoandvehicles_ut60d_ratio
        ,plist_pn_bank_ut180d_ratio
        ,plist_pn_weather_ut90d_all_ratio
        ,plist_pn_mapsandnavigation_ut7d_all_ratio
        ,plist_pn_has_facebook_orca
        ,plist_pn_tools_ut7d_cnt
        ,plist_pn_rloan_ut30d_all_ratio
        ,plist_pn_has_personal_rapi
        ,plist_pn_communication_ut7d_180d_ratio
        ,plist_pn_booksandreference_ut30d_all_ratio
        ,plist_pn_has_dts_freefireth
        ,plist_pn_cashloan_dn500k_ut30d_all_ratio
        ,plist_pn_videoplayers_ut30d_ratio
        ,plist_pn_has_snapchat
        ,plist_pn_entertainment_ut7d_30d_ratio
        ,plist_pn_xiaomi_ut60d_all_ratio
        ,plist_pn_cashloan_dn50k_ut1d_all_ratio
        ,plist_pn_tools_ut60d_ratio
        ,plist_pn_has_supermovil
        ,plist_pn_ut180d_cnt
        ,plist_pn_xiaomi_ut90d_all_ratio
        ,plist_pn_game_ut15d_30d_ratio
        ,plist_install_round_loan_cnt
        ,plist_pn_cashloan_ut1d_90d_ratio
        ,plist_pn_has_docs_editors_sheets
        ,plist_pn_cashloan_rating45_ut90d_ratio
        ,plist_pn_cashloan_rating45_all_ratio
        ,plist_pn_ut60d_ratio
        ,plist_pn_max_perinstallround_ot15d_cnt
        ,plist_pn_has_aliexpresshd
        ,plist_pn_finance_ut60d_ratio
        ,plist_pn_ncategory_ut7d_90d_ratio
        ,plist_pn_finance_ut1d_30d_ratio
        ,plist_pn_has_floyo
        ,plist_pn_cashloan_dn100k_ut15d_cnt
        ,plist_pn_microsoft_ut90d_cnt
        ,plist_pn_photography_ratio
        ,plist_pn_productivity_ut90d_cnt
        ,plist_pn_foodanddrink_ut30d_ratio
        ,plist_pn_ncategory_ut15d_ratio
        ,plist_pn_microsoft_ut15d_all_ratio
        ,plist_pn_booksandreference_ut60d_ratio
        ,plist_pn_cashloan_ratio
        ,plist_pn_loan_ut7d_180d_all_ratio
        ,plist_pn_tools_ut90d_180d_ratio
        ,plist_pn_loan_ut1d_7d_all_ratio
        ,plist_pn_has_nube
        ,plist_pn_loan_ut15d_30d_all_ratio
        ,plist_pn_xiaomi_ut60d_180d_ratio
        ,plist_pn_cashloan_rating3_ut30d_all_ratio
        ,plist_pn_productivity_ut1d_30d_ratio
        ,plist_pn_has_mx_klar
        ,plist_pn_frt200_cnt
        ,plist_pn_musicandaudio_ratio
        ,plist_pn_cashloan_cnt
        ,plist_pn_finance_ut1d_180d_ratio
        ,plist_pn_finservice_ut30d_ratio
        ,plist_pn_rloan_ut1d_all_ratio
        ,plist_pn_motorola_ut7d_all_ratio
        ,plist_pn_investment_ut60d_all_ratio
        ,plist_pn_has_noro
        ,plist_pn_bank_cnt
        ,plist_pn_cashloan_dn10k_ut90d_cnt
        ,plist_pn_has_powerup_stori
        ,plist_pn_t100_musicandaudio_ratio
        ,plist_pn_frt100_szqb_cnt
        ,plist_pn_bank_ut7d_180d_ratio
        ,plist_pn_microsoft_ut30d_ratio
        ,plist_pn_ncategory_ut1d_15d_ratio
        ,plist_pn_communication_ut1d_ratio
        ,plist_pn_foodanddrink_ratio
        ,plist_pn_t20_booksandreference_ratio
        ,plist_pn_t50_productivity_ratio
        ,plist_pn_has_officehubrow
        ,plist_pn_has_nu_production
        ,plist_pn_cashloan_rating4_45_ut180d_all_ratio
        ,plist_pn_has_onsigna_techreo
        ,plist_pn_has_editors_slides
        ,plist_pn_cashloan_rating45_ut180d_ratio
        ,plist_pn_tools_ut180d_ratio
        ,plist_pn_has_instagram_android
        ,plist_pn_business_ratio
        ,plist_pn_lifestyle_ut15d_60d_ratio
        ,plist_pn_frt20_szqb_ratio
        ,plist_pn_bank_ut7d_ratio
        ,plist_pn_finance_ratio
        ,plist_pn_cashloan_dn5m_ut180d_ratio
        ,plist_pn_cashloan_ut7d_180d_all_ratio
        ,plist_pn_loan_ut1d_90d_all_ratio
        ,plist_pn_t200_photography_ratio
        ,plist_pn_cashloan_dn50k_ut15d_60d_ratio
        ,plist_pn_investment_ut60d_180d_ratio
        ,plist_install_round_loan_ut15d_cnt
        ,plist_pn_otafmxloan_1h_cnt
        ,plist_pn_t50_foodanddrink_ratio
        ,plist_pn_ncategory_ut7d_ratio
        ,plist_pn_cashloan_dn50k_ut90d_all_ratio
        ,plist_pn_ncategory_ut60d_90d_ratio
        ,plist_pn_communication_ratio
        ,plist_pn_travelandlocal_cnt
        ,plist_pn_videoplayers_ut60d_ratio
        ,plist_pn_has_convenience
        ,plist_pn_investment_ut60d_ratio
        ,plist_pn_travelandlocal_ut7d_ratio
        ,plist_pn_ebank_ratio
        ,plist_pn_cashloan_ut7d_cnt
        ,plist_pn_bank_ut60d_ratio
        ,plist_pn_cashloan_ut1d_180d_ratio
        ,plist_pn_has_microemu_vtuserapplicationbnrtmb
        ,plist_pn_shopping_ut7d_30d_ratio
        ,plist_pn_cashloan_rating4_45_ut7d_15d_ratio
        ,plist_pn_loan_ut60d_60d_all_ratio
        ,plist_pn_has_client_cartera
        ,plist_pn_cnt
        ,plist_pn_communication_ut60d_ratio
        ,plist_pn_frt20_ratio
        ,plist_pn_rloan_cnt
        ,plist_pn_communication_ut30d_ratio
        ,plist_pn_has_grability
        ,plist_pn_loan_ut1d_7d_ratio
        ,plist_pn_ut7d_90d_ratio
        ,plist_pn_t20_business_ratio
        ,plist_pn_foodanddrink_ut15d_ratio
        ,plist_pn_cashloan_dn100k_ut7d_60d_ratio
        ,plist_pn_has_mx_miapp
        ,plist_pn_t200_shopping_ratio
        ,plist_pn_has_personale_efectivo
        ,plist_pn_cashloan_rating45_ut60d_ratio
        ,plist_pn_cashloan_dn5m_ut30d_ratio
        ,plist_pn_loan_ut1d_180d_all_ratio
        ,plist_pn_personalization_ut60d_ratio
        ,plist_pn_has_mediaclient
        ,plist_pn_ncategory_ut1d_all_ratio
        ,plist_pn_photography_ut30d_180d_ratio
        ,plist_pn_has_google_gm
        ,plist_pn_cashloan_dn1m_ut7d_30d_ratio
        ,plist_pn_utafmxloan_cashloan_cnt
        ,plist_pn_cashloan_dn1m_ut90d_ratio
        ,plist_pn_shopping_ut7d_60d_ratio
        ,plist_pn_social_ratio
        ,plist_pn_cashloan_ut7d_ratio
        ,plist_pn_max_perinstallround_cashloan_cnt
        ,plist_pn_t100_productivity_ratio
        ,plist_pn_has_mercadopago
        ,plist_pn_max_perinstallround_cashloan_ot7d_ratio
        ,plist_pn_ut1d_cnt
        ,plist_pn_has_w4b
        ,plist_pn_bank_ut90d_all_ratio
        ,plist_pn_t100_shopping_cnt
        ,plist_pn_max_perinstallround_loan_cnt
        ,plist_pn_loan_ut1d_90d_ratio
        ,plist_pn_shopping_ut30d_ratio
        ,plist_pn_cashloan_rating4_45_ut30d_all_ratio
        ,plist_pn_loan_ut7d_15d_all_ratio
        ,plist_pn_musicandaudio_ut30d_all_ratio
        ,plist_pn_t20_communication_ratio
        ,plist_pn_utafmxloan_loan_cnt
        ,plist_pn_finmanagement_ut60d_180d_ratio
        ,plist_pn_loan_ut1d_all_ratio
        ,plist_pn_personalization_ut90d_ratio
        ,plist_pn_healthandfitness_ut30d_60d_ratio
        ,plist_pn_has_google_documentsui
        ,plist_pn_musicandaudio_ut30d_180d_ratio
        ,plist_pn_business_ut7d_all_ratio
        ,plist_pn_has_telegram_messenger
        ,plist_pn_education_ut90d_all_ratio
        ,plist_pn_loan_ut7d_90d_ratio
        ,plist_pn_has_bancar
        ,plist_pn_has_spotify_music
        ,plist_pn_has_tachyon
        ,plist_pn_cashloan_dn100k_ut30d_cnt
        ,plist_pn_travelandlocal_ut60d_cnt
        ,plist_pn_cashloan_rating45_ut1d_cnt
        ,plist_pn_finance_ut1d_15d_ratio
        ,plist_install_round_cashloan_cnt
        ,plist_pn_communication_cnt
        ,plist_pn_finmanagement_ut90d_ratio
        ,plist_pn_loan_ut1d_60d_ratio
        ,plist_pn_cashloan_rating45_ut1d_90d_ratio
        ,plist_pn_musicandaudio_ut1d_ratio
        ,plist_pn_ncategory_ut1d_90d_ratio
        ,plist_pn_cashloan_rating4_45_ut60d_ratio
        ,plist_pn_has_android_vending
        ,plist_pn_cashloan_rating3_ut60d_all_ratio
        ,plist_pn_loan_ut1d_180d_ratio
        ,plist_pn_social_ut60d_ratio
        ,plist_pn_samsung_ut90d_ratio
        ,plist_pn_frt100_ydyh_cnt
        ,plist_pn_has_zhiliaoapp_musically
        ,plist_pn_has_youtube_music
        ,plist_pn_max_perinstallround_ot7d_cnt
        ,plist_pn_booksandreference_ut180d_ratio
        ,plist_pn_max_perinstallround_loan_ot15d_cnt
        ,plist_pn_finservice_ut90d_cnt
        ,plist_pn_frt200_ydyh_ratio
        ,plist_pn_utafmxloan_loan_1h_cnt
        ,plist_pn_cashloan_dn1m_ut180d_cnt
        ,plist_pn_cashloan_rating45_ut1d_all_ratio
        ,plist_pn_tools_cnt
        ,plist_pn_has_picsart_studio
        ,plist_pn_finance_ut1d_all_ratio
        ,plist_pn_cashloan_ut180d_all_ratio
        ,plist_pn_t100_game_ratio
        ,plist_pn_cashloan_ut1d_15d_all_ratio
        ,plist_pn_finservice_ut30d_all_ratio
        ,plist_pn_has_eats
        ,plist_pn_ut1d_7d_ratio
        ,plist_pn_cashloan_dn5m_ut60d_ratio
        ,plist_pn_finance_ut180d_ratio
        ,plist_pn_ncategory_ut7d_30d_ratio
        ,plist_pn_has_com_ubercab
        ,plist_pn_has_bazdigitalmovil
        ,plist_pn_has_okredito_rapipeso
        ,plist_pn_ut180d_ratio
        ,plist_pn_musicandaudio_ut30d_ratio
        ,plist_pn_entertainment_ut60d_ratio
        ,plist_pn_tools_ut60d_90d_ratio
        ,plist_pn_game_ut15d_all_ratio
        ,plist_max_appopenwindow_days
        ,plist_pn_cashloan_dn10k_ratio
        ,plist_pn_musicandaudio_ut15d_all_ratio
        ,plist_pn_tools_ut15d_90d_ratio
        ,plist_pn_cashloan_rating45_ut1d_30d_ratio
        ,plist_pn_cashloan_ut1d_all_ratio
        ,plist_pn_ncategory_ut30d_ratio
        ,plist_pn_cashloan_ut1d_30d_ratio
        ,plist_pn_has_sinet_startup_indriver
        ,plist_pn_samsung_ut60d_ratio
        ,plist_pn_has_facebook_lite
        ,plist_an_cnt
        ,plist_pn_has_dla_android
        ,plist_pn_cashloan_dn1m_ut180d_ratio
        ,plist_pn_loan_ut7d_180d_ratio
        ,plist_pn_frt200_ydyh_cnt
        ,plist_pn_musicandaudio_ut60d_all_ratio
        ,plist_pn_entertainment_ut30d_ratio
        ,plist_pn_cashloan_rating4_45_ut7d_ratio
        ,plist_pn_cashloan_rating3_ut180d_all_ratio
        ,plist_pn_xiaomi_ut15d_ratio
        ,plist_pn_finance_ut1d_7d_ratio
        ,plist_pn_shopping_ut7d_all_ratio
        ,plist_pn_social_ut90d_ratio
        ,plist_pn_autoandvehicles_ut15d_ratio
        ,plist_pn_max_perinstallround_ot1d_cnt
        ,plist_pn_mapsandnavigation_ut15d_ratio
        ,plist_pn_has_docs_editors_docs
        ,plist_pn_travelandlocal_ut90d_ratio
        ,plist_pn_cashloan_ut30d_180d_all_ratio
        ,plist_pn_cashloan_dn1m_ut30d_all_ratio
        ,plist_pn_videoplayers_ut180d_ratio
        ,plist_pn_t50_musicandaudio_ratio
        ,plist_pn_shopping_ut90d_all_ratio
        ,plist_pn_has_candycrushsaga
        ,plist_pn_loan_ratio
        ,plist_pn_cashloan_ut15d_90d_ratio
        ,plist_pn_has_apps_docs
        ,plist_pn_lifestyle_ut7d_ratio
        ,plist_pn_finservice_ut90d_ratio
        ,plist_pn_game_ut15d_60d_ratio
        ,plist_pn_cashloan_dn1m_ut30d_ratio
        ,plist_pn_ncategory_ut60d_all_ratio
        ,plist_pn_has_com_baubap
        ,plist_pn_travelandlocal_ut15d_180d_ratio
        ,plist_pn_cashloan_rating4_45_ut7d_all_ratio
        ,plist_pn_productivity_ut180d_ratio
        ,plist_pn_cashloan_dn50k_ut30d_all_ratio
        ,plist_pn_ncategory_ut30d_all_ratio
        ,plist_pn_cashloan_dn1m_all_ratio
        ,plist_pn_tools_ratio
        ,plist_pn_finservice_cnt
        ,plist_pn_xiaomi_ut60d_ratio
        ,plist_pn_finance_ut1d_cnt
        ,plist_pn_has_tiri
        ,plist_pn_has_aplazo
        ,plist_pn_has_avod_thirdpartyclient
        ,plist_pn_ncategory_ut7d_all_ratio
        ,plist_pn_has_speedymovil_wire
        ,plist_pn_has_pagopopmobile
        ,plist_pn_has_clarodrive
        ,plist_pn_cashloan_dn5m_ut90d_ratio
        ,plist_pn_has_financiera
        ,plist_pn_frt100_cnt
        ,plist_pn_tools_ut30d_90d_ratio
        ,plist_pn_has_xiaojukeji_customer
        ,plist_pn_has_apps_chromecast
        ,plist_pn_ensurance_ut30d_ratio
        ,plist_pn_photography_ut7d_all_ratio
        ,plist_pn_cashloan_rating3_all_ratio
        ,plist_pn_cashloan_dn10k_ut7d_all_ratio
        ,plist_pn_finance_ut1d_60d_ratio
        ,plist_pn_has_office_outlook
        ,plist_pn_has_co_nelo_nelo
        ,plist_pn_t100_lifestyle_ratio
        ,plist_pn_entertainment_ut90d_ratio
        ,plist_pn_cashloan_dn10k_ut90d_ratio
        ,plist_pn_cashloan_rating45_ut15d_ratio
        ,plist_pn_frt20_grdk_ratio
        ,plist_pn_max_perinstallround_cashloan_ot1d_ratio
        ,plist_pn_cashloan_rating45_ut30d_ratio
        ,plist_pn_travelandlocal_ut60d_ratio
        ,plist_pn_cashloan_dn1m_ut7d_ratio
        ,plist_pn_has_adobe_reader
        ,plist_pn_has_google_contacts
        ,plist_pn_cashloan_dn100k_ut90d_cnt
        ,plist_pn_cashloan_rating45_cnt
        ,plist_pn_finance_ut30d_ratio
        ,plist_pn_game_ut15d_ratio
        ,plist_pn_utafmxloan_cashloan_3h_cnt
        ,plist_pn_t50_photography_cnt
        ,plist_pn_social_ut180d_ratio
        ,plist_pn_has_facebook_katana
        ,plist_pn_business_ut15d_ratio
        ,plist_pn_has_amazon_mshop_shopping
        ,plist_pn_t20_photography_ratio
        ,plist_pn_finance_ut15d_ratio
        ,plist_pn_personalization_ut180d_all_ratio
        ,plist_pn_xiaomi_ut30d_ratio
        ,plist_pn_t100_social_cnt
        ,plist_pn_has_superapp
        ,plist_pn_musicandaudio_ut15d_ratio
        ,plist_pn_travelandlocal_ut30d_ratio
        ,plist_pn_bank_ut90d_180d_ratio
        ,plist_pn_game_ut90d_all_ratio
        ,plist_pn_investment_ut90d_ratio
        ,plist_pn_has_facil_cash
        ,plist_pn_cashloan_rating45_ratio
        ,plist_pn_cashloan_ut1d_60d_ratio
        ,plist_pn_photography_ut90d_all_ratio
        ,plist_pn_cashloan_ut15d_180d_ratio
        ,plist_pn_finance_ut15d_180d_ratio
        ,plist_pn_has_mbanking
        ,plist_pn_communication_ut180d_ratio
        ,plist_pn_ut1d_30d_ratio
        ,plist_pn_has_google_videos
        ,plist_pn_has_snaptube_premium
        ,plist_pn_tools_ut30d_ratio
        ,plist_pn_lifestyle_ut15d_ratio
        ,plist_pn_foodanddrink_ut180d_ratio
        ,plist_pn_cashloan_ut15d_60d_ratio
        ,plist_pn_ncategory_ut15d_cnt
        ,plist_pn_ccardloan_ut90d_ratio
        ,plist_pn_loan_cnt
        ,plist_pn_microsoft_ut7d_30d_ratio
        ,plist_pn_otbfmxloan_loan_3h_cnt
        ,plist_pn_has_apps_photos
        ,plist_pn_shopping_ut7d_15d_ratio
        ,plist_pn_has_mercadolibre
        ,plist_pn_productivity_ut1d_180d_ratio
        ,plist_pn_booksandreference_ut90d_ratio
        ,plist_pn_tools_ut7d_ratio
        ,plist_pn_has_com_didiglobal_passenger
        ,plist_pn_finance_ut7d_15d_ratio
        ,plist_pn_travelandlocal_ut15d_ratio
        ,plist_pn_rloan_ut30d_30d_all_ratio
        ,plist_pn_bank_ut30d_ratio
        ,plist_pn_has_mx_tala
        ,plist_pn_has_preferido
        ,plist_pn_personalization_ut60d_all_ratio
        ,plist_pn_loan_ut7d_all_ratio
        ,plist_pn_ncategory_ut180d_ratio
        ,plist_pn_t100_social_ratio
        ,plist_pn_has_coppelapp
        ,plist_pn_tools_ut15d_ratio
        ,plist_pn_bank_ut180d_cnt
        ,plist_pn_has_twitter_android
        ,plist_pn_videoplayers_ut30d_all_ratio
        ,plist_pn_musicandaudio_ut180d_ratio
        ,plist_pn_cashloan_dn50k_ut15d_ratio
        ,plist_pn_ut15d_ratio
        ,plist_pn_max_perinstallround_cashloan_ot15d_ratio
        ,plist_pn_bank_ut7d_30d_ratio
        ,plist_pn_social_ut15d_ratio
        ,plist_pn_has_inputmethod_latin
        ,plist_pn_loan_ut1d_30d_ratio
        ,plist_pn_productivity_cnt
        ,plist_pn_ut60d_cnt
        ,plist_pn_finservice_ratio
        ,plist_pn_has_imk
        ,plist_pn_loan_ut7d_cnt
        ,plist_pn_finmanagement_ut60d_ratio
        ,plist_pn_t100_booksandreference_ratio
        ,plist_pn_cashloan_dn500k_ut60d_all_ratio
        ,plist_pn_shopping_ut180d_all_ratio
        ,plist_pn_finservice_ut7d_ratio
        ,plist_pn_cashloan_ut60d_180d_all_ratio
        ,plist_pn_utafmxloan_1h_cnt
        ,plist_pn_t100_business_ratio
        ,plist_pn_ncategory_ut15d_all_ratio
        ,plist_pn_loan_ut1d_15d_ratio
        ,plist_pn_t200_productivity_ratio
        ,plist_pn_productivity_ut15d_all_ratio
        ,plist_pn_t200_personalization_ratio
        ,plist_pn_has_movistarmx
        ,plist_pn_autoandvehicles_ut180d_cnt
        ,plist_pn_has_nbu_files
        ,plist_pn_finservice_ut180d_ratio
        ,plist_pn_social_ut180d_all_ratio
        ,plist_pn_entertainment_ut30d_180d_ratio
        ,plist_pn_productivity_ut90d_ratio
        ,plist_pn_finservice_ut15d_30d_ratio
        ,plist_pn_cashloan_rating3_ut1d_ratio
        ,plist_pn_productivity_ratio
        ,plist_pn_productivity_ut60d_ratio
        ,plist_pn_social_ut7d_all_ratio
        ,plist_pn_microsoft_ut60d_180d_ratio
        ,plist_pn_has_walmart_mg
        ,plist_pn_ncategory_ut180d_all_ratio
        ,plist_install_round_cnt
        ,plist_pn_communication_ut15d_ratio
        ,plist_pn_has_miatt
        ,plist_pn_communication_ut60d_cnt
        ,plist_pn_has_linkedin_android
        ,plist_pn_musicandaudio_ut90d_cnt
    from tmp.feats_20231020_plist_step3_0_f1 a
    left join tmp.feats_20231020_plist_step3_2_f1 b
        on a.job_id=b.job_id and a.user_key=b.user_key
    left join tmp.feats_20231020_plist_step4_1_f2 c
        on a.job_id=c.job_id and a.user_key=c.user_key
    join A_step_users
        on a.user_key = A_step_users.user_key
    where A_step_users.create_time > a.http_timestamp
)
insert overwrite table tmp.lizhonghao_offline_plist_check
SELECT `(f)?+.+`
from features
where f = 1
--!hive
"""
)
df_offline = hive.execute("select * from tmp.lizhonghao_offline_plist_check")
df_offline.to_csv(OffLINE_FILE, index=False)

hive.close()


# checkpoint
df_online: pd.DataFrame = pd.read_csv(ONLINE_FILE)
df_offline: pd.DataFrame = pd.read_csv(OffLINE_FILE)

# check feature counts
# df_online.columns = [col.split('.')[-1] for col in df_online.columns]
lst_features = [feature
                for feature in df_online['key'].unique()
                if feature.startswith(FEATURE_PREFIX)]

# FEATURE INTEGRITY
assert N_FEATURES == len(lst_features)
# there are 4 non-feature keys: user_key, capture_time, job_id and label_output_time
assert all(df_online.groupby(["dt","verify_key"])["key"].nunique() == N_FEATURES + 4)


# online table long to wide
df_online = df_online[df_online["dt"] == '2023-10-21']
df_dt = df_online[["user_key", "dt"]].drop_duplicates()
df_online = df_online.pivot_table(index='verify_key', columns='key', values='value', aggfunc='first')
# df_online = pd.merge(df_online, df_dt, on=["user_key"])


# data clean
df_online["capture_time"] = pd.to_datetime( 
    df_online["capture_time"].map(lambda x: dt.datetime.fromtimestamp(float(x) / 1e3))
)
# df_online = df_online[df_online["capture_time"] >= TILL_CHN_TIME]

# df_offline.columns = [col.split('.')[-1] for col in df_offline.columns]
df_offline.set_index("loan_key", inplace=True)
df_offline["http_timestamp"] = pd.to_datetime(df_offline["http_timestamp"])
# df_offline = df_offline[df_offline["http_timestamp"] >= TILL_MEX_TIME]
df_offline = df_offline[df_offline["dt"] == '2023-10-21']


# USER INTEGRITY
assert (~df_offline.index.isin(df_online.index)).sum() == 0
assert (~df_online.index.isin(df_offline.index)).sum() == 0
# if assertiong fails, might check mismatched records
df_offline[["dt", "user_key", "http_timestamp", "dt"]][~df_offline.index.isin(df_online.index)]
df_online[["user_key", "capture_time", "job_id"]][~df_online.index.isin(df_offline.index)]
df_offline[["dt", "user_key", "http_timestamp"]][~df_offline.user_key.isin(df_online.user_key)]
df_online[["user_key", "capture_time", "job_id"]][~df_online.user_key.isin(df_offline.user_key)]
# output for report
df_online.to_csv(ONLINE_VAR_FILE)
df_offline.to_csv(OFFLINE_VAR_FILE)


# reorder features, prepare for value integrity check
df_online_feature = df_online[lst_features].astype('float16')
df_offline_feature = df_offline[lst_features].astype('float16')
# df_online_feature = df_online_feature[df_online_feature.index.isin(df_offline.index)]
# df_offline_feature = df_offline_feature[df_offline_feature.index.isin(df_online.index)]

# get differenciated values across the whole data
df_diff = abs(df_online_feature - df_offline_feature)
df_diff.fillna(np.inf, inplace=True)
df_bool_diff = df_diff >= NUM_IS_CLOSE
# or try more stricted rule using np.isclose
# df_online_feature = df_online_feature.loc[df_offline_feature.index]
# df_bool_diff = pd.DataFrame(
#     ~np.isclose(df_online_feature, df_offline_feature),
#     index=df_offline_feature.index,
#     columns = df_offline_feature.columns
# )


# calculate percentage of distinguished numbers for all features
srs_pct_diff = df_bool_diff.sum(axis=0) / df_bool_diff.shape[0]
srs_pct_diff.sort_values(ascending=False, inplace=True)
(1. - srs_pct_diff).to_csv(PCT_MATCH_VAR_FILE)

# VALUE INTEGRITY
# check if nan exists in online features
assert ~df_online_feature.isna().any().any()

# if exists, check specific features
df_online_feature.loc[df_online_feature.isna().any(axis=1), df_online_feature.isna().any(axis=0)]

# check if percentage of differences are acceptable
assert (srs_pct_diff >= ACCEPT_RATE).sum() == 0

# if not, might check specific numerics in these features that make the differences
srs_pct_diff = srs_pct_diff[srs_pct_diff > 0.]
srs_pct_diff

# check by feature name
col = "plist_pn_ut1d_7d_ratio"
pd.concat([
    df_offline["user_key"][df_bool_diff[col]],
    df_online["capture_time"][df_bool_diff[col]],
    df_online_feature[col][df_bool_diff[col]],
    df_offline_feature[col][df_bool_diff[col]]
    ],
    axis=1,
    copy=False
)

# save mismatched records on-the-fly
lst_mismatched_cols = []
for col in srs_pct_diff.index:
    df_mismatch = pd.concat([
        # also add user_key to csv file
        df_offline["user_key"][df_bool_diff[col]],
        df_online["capture_time"][df_bool_diff[col]],
        df_online_feature[col][df_bool_diff[col]],
        df_offline_feature[col][df_bool_diff[col]]
        ],
        axis=1,
        copy=False
    )
    df_mismatch.index.name = "loan_key"

    display(df_mismatch)
    if input("press enter to judge as mismatched feature") == '':
        lst_mismatched_cols.append(col)
        df_mismatch.to_csv(os.path.join(
            DIR_WORK_DATA, "{}_{}.csv".format(col, str(srs_pct_diff[col])[:4])
        ))
