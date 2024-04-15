# -*- coding: utf-8 -*-
"""
Created on Wed Feb 21 18:36:28 2024

@author: jislam182
"""

from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying, flatten_dict_column
import pandas as pd
from typing import List
import urllib.parse
import io
from tqdm import tqdm
import time

start_time=time.time()
tableau_config={
    
    'tableau_prod': {
        'server': 'https://10ax.online.tableau.com/',
        'api_version': '3.22',
        "personal_access_token_name": "Restapi",
		"personal_access_token_secret": "TyhYvN0rR7ODseIzZLMMgA==:gXGumMgnhfjaTZ0liREMtPF8rMGYXKvH",
        'site_name': 'jahidulislamrestapitesting',
        'site_url': 'jahidulislamrestapitesting',
    }
    }


connection = TableauServerConnection(config_json=tableau_config, env='tableau_prod')

response=connection.sign_in()
print (response)
print('-------------You have successfully logged into the Tableau Server---------------')

# Finding Workbook
Workbook_name='PDF Assessment'
view_Name='CompanyName'
pdf_view_Name ='CompanyBreakdown'

# Filter List
# 2017,2018,2019,2020,2021,2022,2023
Current_Year='2022'

#Company Target,Industry Average,Portfolio Average
Benchmark='Industry Average'


#PDF Page orientation
#Portrait or Landscape
pdf_orientation= "Landscape"

#PDF Page
#A3, A4, A5, B5, Executive, Folio, Ledger, Legal, Letter, Note, Quarto, or Tabloid.
page_type="Letter"




def View_id(Workbook_name,view_Name,connection):
    df_view= querying.get_views_dataframe(connection)
    df_view= flatten_dict_column(df_view, keys=["name","id"], col_name= "workbook")
    df_view = df_view[['workbook_name','contentUrl','viewUrlName','id']].copy()
    df_view = df_view[(df_view['workbook_name'] == Workbook_name) & (df_view['viewUrlName'] == view_Name)]
    view_id_num= df_view[['id']].copy()
    view_id_num=view_id_num.values[0]
    for i in view_id_num:
        view_id_num=i
    return (view_id_num)

def export_csv(view_id_num,connection):
    view_CSV= connection.query_view_data(view_id=view_id_num)
    global view_Name
    global Workbook_name
    #with open(f"CSV_list_{Workbook_name}={view_Name}.csv",'wb') as file:
        #file.write(view_CSV.content)
    #print("=======================")
    Company_list=pd.read_csv(io.StringIO(view_CSV.text))
    Company_list=Company_list[['Company Name']].copy()
    Company_list=Company_list['Company Name'].tolist()
    #Company_list=map(str, Company_list)
    return Company_list
  

def export_pdf(pdf_view_id_num,Company_list_1, Current_Year,Benchmark,pdf_orientation,page_type,Company_Name):
    #filters
    
    filter_1=urllib.parse.quote("Current Year")
    filter_2=urllib.parse.quote("Benchmark")
    filter_3=urllib.parse.quote("Companyid")
    filter_1value=urllib.parse.quote(Current_Year)
    filter_2value=urllib.parse.quote(Benchmark)
    filter_3value=urllib.parse.quote(Company_list_1)

    custom_url_params={

            "Report_year": f"vf_{filter_1}={filter_1value}",
            "Report_type": f"vf_{filter_2}={filter_2value}",
            "Company_name": f"vf_{filter_3}={filter_3value}",
            "pdf_page_type": f"type={page_type}",
            "pdf_orientation": f"orientation={pdf_orientation}"
            }
    exp_pdf= connection.query_view_pdf(view_id=pdf_view_id_num,parameter_dict=custom_url_params)
    
    with open(f"{Benchmark} report for {Company_Name} {filter_1value}.pdf",'wb') as file:
            file.write(exp_pdf.content)
    
    #print(connection.active_endpoint)
    #print("end point")


view_id_num=View_id(Workbook_name,view_Name,connection)
Company_list=export_csv(view_id_num,connection)
pdf_view_id_num=View_id(Workbook_name,pdf_view_Name,connection)

progbar=tqdm(total=len(Company_list))
for ind, Company_Name in enumerate (Company_list):
    ind= ind+1
    i=str(ind)
    progbar.update(1)
    progbar.set_description(f'Processing {Company_Name} report')
    export_pdf(pdf_view_id_num,i, Current_Year,Benchmark,pdf_orientation,page_type,Company_Name)

progbar.close()
total_time=time.time()- start_time

print (f'To Complete the full Program We need {total_time} S')
connection.sign_out()