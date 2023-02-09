import cx_Oracle
import pandas as pd
import warnings
import logging

#import math

import orcl



if __name__ == "__main__":


    logging.basicConfig(filename="bio_attn_admin_building.log",
                        format='%(asctime)s %(message)s',
                        filemode='w')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    pd.set_option('mode.chained_assignment', None)

    # Change the name of excel file as per the saved file name
    df = pd.read_excel('d:/1.xlsx', skiprows=[0, 1, 2, 3, 4])
    stage_1 = df[['EmployeeNo', 'Date', 'In', 'Out']]
    stage_2 = stage_1[stage_1['EmployeeNo'] != 'Summary']
    stage_3 = stage_2[~(stage_2['Date'].astype(str).str.contains('NaT'))]

    emp_code = stage_3.iloc[0, 0]

    for i in range(stage_3.shape[0]):
        emp_no = stage_3.iloc[i, 0]

        if (type(emp_no) == int):

            emp_no = emp_code
        else:
            stage_3.iloc[i, 0] = stage_3.iloc[i - 1, 0]

    stage_3['atten_flag'] = ''

    for i in range(stage_3.shape[0]):

        # print(type(stage_3.iloc[i,2]))
        if (type(stage_3.iloc[i, 2]) == float):
            stage_3.iloc[i, 4] = "ABSENT"
        else:
            stage_3.iloc[i, 4] = "PRESENT"


    stage_4 = stage_3.dropna(subset=['In'], how='all')
    stage_4 = stage_4.astype(str)
    stage_4.to_excel("K:/Datacore/DGP/admin_building_attn.xlsx",index=False)

    try:

        con = orcl.make_connection()
        cursor_prod = con.cursor()
        # delete the current records
        delete_existing_data = "delete from admin_buiding_attn"
        cursor_prod.execute(delete_existing_data)
        con.commit()

        for i in range(stage_4.shape[0]):
            emp_code = stage_4.iloc[i, 0]
            attn_date = stage_4.iloc[i, 1]
            in_time = stage_4.iloc[i, 2]
            out_time = stage_4.iloc[i, 3]
            attn_status = stage_4.iloc[i, 4]

            insert_query = f"insert into admin_buiding_attn(emp_no,attn_date,in_time,out_time,attn_status) values('{emp_code}','{attn_date}','{in_time}','{out_time}','{attn_status}')"
            # print(insert_query)
            cursor_prod.execute(insert_query)
        cursor_prod.close()
        con.commit()
        con.close()

    except Exception as e:
        print(e)
        logger.debug(e)