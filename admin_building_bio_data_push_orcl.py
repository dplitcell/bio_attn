import cx_Oracle
import pandas as pd
import warnings
import logging
import orcl

if __name__ == "__main__":

    logging.basicConfig(filename="bio_attn_admin_building.log",
                        format='%(asctime)s %(message)s',
                        filemode='w')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    pd.set_option('mode.chained_assignment', None)

    # Change the name of excel file as per the saved file name
    df = pd.read_excel('d:/6.xlsx', skiprows=[0, 1, 2, 3, 4])
    stage_1 = df[['EmployeeNo', 'Date', 'In', 'Out']]
    stage_2 = stage_1[stage_1['EmployeeNo'] != 'Summary']
    stage_3 = stage_2[~(stage_2['Date'].astype(str).str.contains('NaT'))]

    emp_code = stage_3.iloc[0, 0]

    for i in range(stage_3.shape[0]):
        emp_no = stage_3.iloc[i, 0]

        if type(emp_no) == int:

            emp_no = emp_code
        else:
            stage_3.iloc[i, 0] = stage_3.iloc[i - 1, 0]

    stage_3['atten_flag'] = ''

    for i in range(stage_3.shape[0]):

        if type(stage_3.iloc[i, 2]) == float:
            stage_3.iloc[i, 4] = "ABSENT"
        else:
            stage_3.iloc[i, 4] = "PRESENT"

    stage_4 = stage_3.dropna(subset=['In','Out'], how='all')


    #stage_4.loc[:, 'Out'] = pd.to_datetime(stage_4.loc[:, 'Out']stage_4["Date"]+" "+stage_4["Out"])
    #stage_4.loc[:, 'In'] = pd.to_datetime(stage_4.loc[:, 'In'])
    #df["In"] = df["In"].apply(pd.Timestamp)  # will handle parsing
    #df["Out"] = df["Out"].apply(pd.Timestamp)  # will handle parsing
    #stage_4['day_diff'] = stage_4["In"] - stage_4["Out"]

    #stage_4['delta'] = stage_4['stop'] - stage_4['start']
    try:
        stage_4 = stage_4.astype(str)
        stage_4["Out"].replace('nan', '10:00:00', inplace=True)
        stage_4["In"].replace('nan', '17:00:00', inplace=True)
        stage_4["In-dt"] = stage_4["Date"] + stage_4["In"]
        stage_4["Out-dt"] = stage_4["Date"] +  stage_4["Out"]

        stage_4["In-dt"] = pd.to_datetime(stage_4["In-dt"], format='%Y-%m-%d%H:%M:%S')
        stage_4["Out-dt"] = pd.to_datetime(stage_4["Out-dt"], format='%Y-%m-%d%H:%M:%S')

        stage_4['duty_hours'] = (stage_4["Out-dt"] - stage_4["In-dt"]).dt.total_seconds()/3600
    except Exception as e:
        print(e)

    stage_4["Out"].replace('nan', ' ', inplace=True)
    stage_4["In"].replace('nan', ' ', inplace=True)
    temp_df_1 = stage_4[["EmployeeNo","In-dt","Date"]]
    temp_df_1['in_out_flag']='1'
    temp_df_2= stage_4[["EmployeeNo", "Out-dt","Date"]]
    temp_df_2['in_out_flag'] = '2'
    temp_df_1.rename(columns={"In-dt": "punch_time","Date":"attn_date"}, inplace=True)
    temp_df_2.rename(columns={"Out-dt": "punch_time","Date":"attn_date"}, inplace=True)
    result_df = pd.concat([temp_df_1, temp_df_2])
    result_df.sort_values(["EmployeeNo", "punch_time"],inplace=True)
    stage_4.to_excel("K:/Datacore/DGP/test.xlsx",index=False)
    result_df.to_excel("K:/Datacore/DGP/per_day.xlsx", index=False)

    try:

        con = orcl.make_connection()
        cursor_prod = con.cursor()

        cpu_id = "ADM"

        for i in range(result_df.shape[0]):
            emp_code = result_df.iloc[i, 0]
            attn_date = result_df.iloc[i, 2]
            punch_time = result_df.iloc[i, 1]
            in_out_flag = result_df.iloc[i, 3]
            insert_query_datacore = f"insert into BIO_ATTN_LIVE_DATA(emp_no,attendance_date,punch_time,punch_time_str,in_out_code,cpu_id) values('{emp_code}',to_date('{attn_date}','yyyy-mm-dd'),to_date('{punch_time}','yyyy-mm-dd hh24:mi:ss'),'{punch_time}','{in_out_flag}','{cpu_id}')"
            # print(insert_query)
            cursor_prod.execute(insert_query_datacore)
        cursor_prod.close()
        con.commit()
        con.close()

    except Exception as e:
        print(e)
        logger.debug(e)