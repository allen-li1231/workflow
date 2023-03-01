import pandas as pd
from tqdm import tqdm

from workflow import hue
from workflow.hue import Notebook
from workflow.settings import PROGRESSBAR
from workflow.jump_server import SSH, SFTP

# from workflow.model_monitor.reports import binary_classification_report

# df_X = pd.read_excel(f"W:/LFTFS/git-repo/public-economic-crime/work_data/features_20220929.xlsx", index_col=0)
# df_risky_control = pd.read_excel(r"Z:\监控组\2022涉众模型\risky_merchant_20220929.xlsx", sheet_name="summary")
# df_X = df_X[df_X.index.isin(df_risky_control["merchant_id"])]
# res = binary_classification_report(y_true=df_risky_control["will_control"],
#                                    y_score=df_risky_control["pred"],
#                                    x=df_X,
#                                    threshold=0.7)

# jump_server_username, username = "lizhonghao", "deploy"
# jump_server_password, password = "Lzh748@lakala", "0bT_h8C-Ru19C4lH"
# ssh = SSH(username, password, jump_server_username, jump_server_password)
# sftp = SFTP(username, password, jump_server_username, jump_server_password)
# sftp.get('/home/fengkong/neo4j-community-3.5.3/import/out_sourcing_rel/wb_nodes.csv', './wb_nodes.csv',)
# sftp.get('/home/fengkong/neo4j-community-3.5.3/import/out_sourcing_rel/wb_relationship.csv', './wb_relationship.csv',)
# sftp.put('W:/requests_toolbelt-0.10.0-py2.py3-none-any.whl', '/home/deploy/requests_toolbelt-0.10.0-py2.py3-none-any.whl')
# sftp.put('W:/libaio-0.3.109-13.el7.x86_64.rpm', '/home/deploy/libaio-0.3.109-13.el7.x86_64.rpm')

from workflow.jupyter import Jupyter
j = Jupyter()
#
#
# # shell 运行指令
# conn = j.connect_terminal("1")
# conn.execute("ls -al lizhonghao")
#
# # 上传
# j.upload(file_path="./utils.py", dst_path="lizhonghao")
# # 下载
# j.download(file_path="./utils.py", dst_path="lizhonghao")
#
# file_path = 'lizhonghao/public-economic-crime/work_data/woe_features.pkl'
# dst_path = './woe_features.pkl'
# res = j._download(file_path)
# j.upload("./utils.py", "lizhonghao")

h = hue("lizhonghao", "Lzh748@lakala", name="Test", verbose=True)
# h.download("buff_fk.lzh_tmp_test_490000",
#            reason="deploy_public_economic_crime",
#            path="./test_deploy_trans.csv")

s = """
--{q}
select {q},
a.acct_no,
count(distinct merchant_id) mer_num,
sum(trans_amount/10000000) tr_amt,
count(order_id) tr_cnt,
sum(case when card_type in ('01','02') then trans_amount/10000000 else 0 end) credit_amt,
sum(case when card_type in ('00') then trans_amount/10000000 else 0 end) debit_amt,
round(sum(case when card_type in ('01','02') then trans_amount/10000000 else 0 end)
/sum(trans_amount/10000000),4) as credit_amt_zb,
round(sum(case when card_type in ('00') then trans_amount/10000000 else 0 end)
/sum(trans_amount/10000000),4) as debit_amt_zb,
round(sum(trans_amount/10000000)/count(order_id),4) amt_per
from dwf.offline_pay_order a
where ymd between date_format(date_sub(current_date(),7),'yyyyMMdd') and date_format(date_sub(current_date(),1),'yyyyMMdd')
      and trans_code = '012001'
	  and card_app_type <>'200307'
	  and rep_code='00'
	  --and substr(innet_time,1,10)>='2020-01-01'
	  and trans_amount/1000>10
and a.acct_no not in (select b.acct_no from buffer_fk.zyy_xq_credit_add b)
group by a.acct_no
having mer_num>={q}
      and sum(trans_amount/10000000)>=50
	  and round(sum(case when card_type in ('01','02') then trans_amount/10000000 else 0 end)
/sum(trans_amount/10000000),4)>0.6
and round(sum(trans_amount/10000000)/count(order_id),4) between 1 and 5;
"""

# res = h.run_sqls(["sele", "select * from buff_fk.lzh_test_workflow"])

# lst_res = h.batch_download(["buff_fk.lzh_test_workflow", "buff_fk.lzh_test_workflow", "buff_fk.lzh_test_workflow"],
#                            reasons="test workflow",
#                            columns=[["acct_no"], ["acct_no", "mer_num"],
#                                     ["acct_no", "mer_num", "tr_amt", "tr_cnt", "credit_amt", 'bad_column']],
#                            decrypt_columns=[[], ["acct_no"], ["acct_no"]],
#                            use_hue=True)
#
# df_trans = h.get_table("buff_fk.lzh_public_economic_crime__test_cash_laundry_trans",
#                        rows_per_fetch=300)
#
# res = h.run_sql(s.format(q=0), print_log=False)
#
# h.run_sql(
#     "create table buff_fk.lzh_tmp_test_655350 as select * from buff_fk.lzh_deploy_public_economic_crime__trans limit 655350")
# h.download("buff_fk.lzh_tmp_test_655350",
#            reason="deploy_public_economic_crime",
#            path="./test_deploy_trans.csv")

# df = pd.DataFrame(**h.run_sql("show tables;", database="buff_fk").fetchall())
# for name in df['tab_name']:
#     if "lizhonghao" in name:
#         print(name)

# df = h.download("buff_fk.ud_lizhonghao_1661221807828_3589", "test")
# h.upload(df, "test", encrypt_columns=["regist_name", "corporate_representative"])


# df = pd.read_excel("relevance_public_crime_opinion_20220818.xlsx")

# h.upload(df, "Test", )
# h.upload("relevance_public_crime_opinion_20220818.xlsx")


# if __name__ == "__main__":
#     # h.run_notebook_sqls([s.format(q=0), s.format(q=1)])
#     df = h.get_table("buff_fk.lzh_test_workflow", "test", columns=["acct_no"], path="test.xlsx")
#     res = h.batch_download(["buff_fk.lzh_test_workflow"] * 4,
#                            "test", )
#     print("start")
#     sql_tasks = {"0": [s.format(q=0), s.format(q=1)],
#                  "1": [s.format(q=0), s.format(q=1), s.format(q=2)],
#                  "2": [s.format(q=2)]}
#     pbar_setup = PROGRESSBAR.copy()
#     pbar_setup["total"] = len(sql_tasks)
#     pbar_setup["desc"] = "start sql executions"
#     with tqdm(position=0, **pbar_setup) as pbar:
#         for task, sqls in sql_tasks.items():
#             if task == '1':
#                 pbar.update(1)
#
#             pbar.set_description(f"performing {task}")
#             h.run_sqls(sqls, progressbar_offset=1)
#             pbar.update(1)
#
#     print("OK")
    # df = h.get_table("buff_fk.ud_lizhonghao_1661405700307_3610", columns=["regist_name"],
    #                  decrypt_columns=["regist_name"])
    # l = [res.fetchall() for res in r]

    # nb1 = h.hue_sys.new_notebook("nb1", verbose=True)
    # nb2 = h.hue_sys.new_notebook("nb2", verbose=True)
    # res1 = nb1.execute("set hive.execution.engine", print_log=True)
    # res2 = nb2.execute("set hive.execution.engine", print_log=True)

    # h.hue_sys.set_engine("spark")
    # res = h.run_notebook_sql(s.format(q=1), print_log=True)
    # h.kill_app(res.app_id)
    # h.close()
