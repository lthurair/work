######## DEFINE VARS ################
rec_date='20240112'

curr_date=int(rec_date)
from datetime import datetime
dteq = datetime.strptime(rec_date, '%Y%m%d').strftime('%Y-%m-%d')

########## CHANGE EVERY FISCAL YEAR #############
m1='202311' ### start of fiscal year
fy_start='20231101'

############################## CALCULATE THE PLANS #################################################

### NUMBER OF DAYS IN QUARTER
q1_st = date(2023, 11, 1)
q1_ed = date(2024, 2, 1)
q1_tot = q1_ed - q1_st

q2_st = date(2024, 2, 1)
q2_ed = date(2024, 5, 1)
q2_tot = q2_ed - q2_st

q3_st = date(2024, 5, 1)
q3_ed = date(2024, 8, 1)
q3_tot = q3_ed - q3_st

q4_st = date(2024, 8, 1)
q4_ed = date(2024, 11, 1)
q4_tot = q4_ed - q4_st


###### DAYS PASSED IN QUARTER
tuple_q1=(11,12,1)
tuple_q2=(2,3,4)
tuple_q3=(5,6,7)
tuple_q4=(8,9,10)

import datetime
mth0= pd.to_datetime(dteq, format='%Y-%m-%d')
mth00 = mth0.month

rec_date = datetime.datetime.strptime(dteq, '%Y-%m-%d').strftime('%Y%m%d')

d2=datetime.datetime.strptime(dteq, '%Y-%m-%d').date()

if mth00 in tuple_q1: 
    d0 = date(2023, 11, 1)
    q1_passed=(d2-d0)
    q2_passed=(d2-d2) ###need 0 in datetime
    q3_passed=(d2-d2) ###need 0 in datetime
    q4_passed=(d2-d2) ###need 0 in datetime
elif mth00 in tuple_q2: 
    d0 = date(2024, 2, 1)
    q2_passed=(d2-d0)
    q1_passed=(q1_tot)
    q3_passed=(d2-d2)
    q4_passed=(d2-d2)
elif mth00 in tuple_q3: 
    d0 = date(2024, 5, 1)
    q3_passed=(d2-d0)
    q1_passed=(q1_tot)
    q2_passed=(q2_tot)
    q4_passed=(d2-d2)
elif mth00 in tuple_q4:
    d0 = date(2024, 8, 1)
    q4_passed=(d2-d0)
    q1_passed=(q1_tot)
    q2_passed=(q2_tot)
    q3_passed=(q3_tot)

#######################RUN THE FUNCTIONS #########################

########## FILTER RELEVANT VARIABLES (SBB program)
exec("""def filterfunc(work): 
    return work[['psa_month','metric_name','pr_acf2','employee_name','start_date','end_date',
                 'elig_ind', 'seg_ind','transaction_id','original_sales_transaction_id', 'product_code',
                 'recognition_date','sale_date','current_amount','branch_no','am_cost_center',
                 'am_cost_center_name','am_cost_center_full_name','sm_cost_center','sm_cost_center_name',
                 'record_date']]
                    """)

########## FILTER RELEVANT VARIABLES (Retail program)
exec("""def filterfunc1(work): 
    return work[['psa_month','metric_name','pr_acf2','employee_name',
                 'elig_ind', 'seg_ind','transaction_id','original_sales_transaction_id', 'product_code',
                 'recognition_date','sale_date','current_amount','branch_no','am_cost_center',
                 'am_cost_center_name','am_cost_center_full_name','sm_cost_center','sm_cost_center_name',
                 'record_date']]
                    """)

## CREATE THE QTD AND YTD VARIABLES
exec("""def newvarfunc(work1): 
    work1['q1'] = (work1.november.fillna(0)) + (work1.december.fillna(0)) + (work1.january.fillna(0))
    work1['q2'] = (work1.february.fillna(0)) + (work1.march.fillna(0)) + (work1.april.fillna(0))
    work1['q3'] = (work1.may.fillna(0)) + (work1.june.fillna(0)) + (work1.july.fillna(0))
    work1['q4'] = (work1.august.fillna(0)) + (work1.september.fillna(0)) + (work1.october.fillna(0))
    work1['ytd'] = (work1.q1.fillna(0)) + (work1.q2.fillna(0)) + (work1.q3.fillna(0)) + (work1.q4.fillna(0))
    """)


### AMSB GROUPBY -- used
exec("""def grpfunc(work): 
    return work.groupby(by=['metric_name','acf2_id','employee_name',
                 'am_cost_center','am_cost_center_name','am_cost_center_full_name',
                 'sm_cost_center','sm_cost_center_name','record_date'],as_index=False).aggregate({
                        'november': 'sum',
                        'december': 'sum',
                        'january': 'sum',
                        'february': 'sum',
                        'march': 'sum',
                        'april': 'sum',
                        'may': 'sum',
                        'june': 'sum',
                        'july': 'sum',
                        'august': 'sum',
                        'september': 'sum',
                        'october': 'sum'
                     })""")

### AM GROUPBY
exec("""def amgrpfunc(work1): 
    return work1.groupby(by=['metric_name','am_cost_center','am_cost_center_name','am_cost_center_full_name',
                             'sm_cost_center','sm_cost_center_name','record_date'],as_index=False).aggregate({
                    'november': 'sum',
                     'december': 'sum',      
                     'january': 'sum',
                     'february': 'sum',
                     'march': 'sum',
                     'april': 'sum',
                     'may': 'sum',
                     'june': 'sum',
                     'july': 'sum',
                     'august': 'sum',
                     'september': 'sum',
                     'october': 'sum'
                     })
    """)
    

### SM GROUPBY
exec("""def smgrpfunc(work3): 
    return work3.groupby(by=['metric_name','sm_cost_center','sm_cost_center_name','record_date'],as_index=False).aggregate({
                    'november': 'sum',
                     'december': 'sum',      
                     'january': 'sum',
                     'february': 'sum',
                     'march': 'sum',
                     'april': 'sum',
                     'may': 'sum',
                     'june': 'sum',
                     'july': 'sum',
                     'august': 'sum',
                     'september': 'sum',
                     'october': 'sum'
                     })
    """)


## GROUP BY - NATIONAL OFFICE - used
exec("""def natgrpfunc(work): 
    return work.groupby(by=['metric_name'],as_index=False).aggregate({
                        'november': 'sum',
                        'december': 'sum',
                        'january': 'sum',
                        'february': 'sum',
                        'march': 'sum',
                        'april': 'sum',
                        'may': 'sum',
                        'june': 'sum',
                        'july': 'sum',
                        'august': 'sum',
                        'september': 'sum',
                        'october': 'sum'
                     })""")


########################################### OLD ################################################


########## GNB - KEEP RELEVANT VARS
exec("""def gnbfilterfunc(work): 
    return work[['metric_name','acf2_id','slsbran','prodcd','salebal','franchise_cust',
                 'professional_cust','month_dt','gnb','auth_credit','gnb_volume',
                 'gnb_points','job_code','am_cost_center','region_id','record_date']]
                    """)


############################### GROUP BY TO GET TOTAL ###################################
    
### GROUPBY -- used
exec("""def allgrpfunc(work): 
    return work.groupby(by=['metric_name','level','acf2_id','employee_name',
                 'am_cost_center','am_cost_center_name','am_cost_center_full_name',
                 'sm_cost_center','sm_cost_center_name'],as_index=False).aggregate({
                        'november': 'sum',
                        'december': 'sum',
                        'january': 'sum',
                        'Q1': 'sum',
                        'february': 'sum',
                        'march': 'sum',
                        'april': 'sum',
                        'Q2': 'sum',
                        'may': 'sum',
                        'june': 'sum',
                        'july': 'sum',
                        'Q3': 'sum',
                        'august': 'sum',
                        'september': 'sum',
                        'october': 'sum',
                        'Q4': 'sum',
                        'YTD': 'sum'
                     })""")



### RUN THE PRODUCT LIST
#################################(1) GROSS VOLUME ###################################

## TOTAL GROSS VOLUME
gross_all_tuple=('S610','S612','S615','S617','S620','S625','S630',
                'S635','S640','S645','L530','L550','L555','L570',
                'L600','L605','L630','S660','S662','S663','S667',
                'S668','S670','S675','S680','S690','S650','S021',
                'S026','L290')

## GROSS LOC VOLUME
gross_loc_tuple=('S610','S612','S615','S617','S620','S625','S630','S635',
                 'S640','S645','S021','S026')

## GROSS LON VOLUME
gross_lon_tuple=('L530','L550','L555','L570','L600','L605','L630','S660',
                 'S662','S663','S667','S668','S670','S675','S680','S690',
                 'S650','L290')

## GROSS STUDENT LOC VOLUME
gross_student_loc_tuple=('S021','S026','L290')

## PROFESSIONAL 
professional_tuple=('L575','L700','L710','S021','S026','L290')

## GROSS HEALTH CARE VOLUME - doesn't exist right now
gross_health_tuple=('Z1234',)

##################################GROSS SEGMENTATION METRICS ########################

gross_seg_tuple=('SBB Credit Widgets - Deals $250M - <$500M','SBB Credit Widgets - Deals $75M - <$250M',
                 'SBB Credit Widgets - Deals <=$75M','SBB Credit Widgets - Deals >=$500M')

#################################(2) CREDIT CARDS ###################################

## TOTAL CREDIT CARDS
total_cc_tuple=('V940','V990','V992','V930','V950')

## BUSINESS VISA
business_visa_cc_tuple=('V940',)

## BUSINESS TRAVEL VISA
business_travel_tuple=('V990',)

## AEROPLAN BUSINESS
business_aero_tuple=('V992',)

## BUSINESS SELECT NO FEE
bus_nofee_tuple=('V930',)

## BUSINESS SELECT FEE
bus_fee_tuple=('V950',)

#################################(3) MERCHANT SOLUTIONS ###################################

## TOTAL MERCHANT SOLUTIONS
total_mer_tuple=('V910','V960','V970')

## MERCHANT VISA
mer_visa_tuple=('V910',)

## REFER MERCH VISA CBC
refer_mer_cbc_tuple=('V960',)

## REF MERCH VSA RETAIL
refer_mer_retail_tuple=('V970',)

#################################(4) CMS ################################################

## TOTAL CMS
total_cms_tuple=('C010','C020','C030','C040')

## EDI/USBBS/CC/IWPN
#edi_usbbs_cms_tuple=('C010',)

## BR/CRS/USBBS-CM
#br_crs_cms_tuple=('C020',)

## EFT/WP/USBBSACH
#eft_wp_cms_tuple=('C030',)

## LBX/EDI-ORG
#lbx_edi_cms_tuple=('C040',)

#################################(5) COMMERCIAL VOLUME #################################

## TOTAL COMMERCIAL VOLUME
total_com_tuple=('D909','L899','L973','L999')

## COM DEPOSIT FILTER
com_deposit_tuple=('D909',)

## COM LOAN FILTER
com_loan_tuple=('L899','L973','L999')

#################################(6) NEW ACCOUNT OPENINGS #############################

## TOTAL ACCOUNT OPENINGS
total_new_tuple=('D915',)

#################################(6) TD SECURITIES DIRECT TRADE #######################

## TD SECURITIES DIRECT TRADE
total_trade_tuple=('X810','X815')

#################################(7) BCP WIDGETS (BCLI) ##############################

## BCP WIDGETS (BCLI)
total_bcp_tuple=('I050',)

#################################(8) MUR MORTGAGE ####################################

## MUR MORTGAGE VOLUME
total_mur_tuple=('M800','M850')

#################################(9) WEALTH REFERRALS #################################

## WEALTH REFERRAL WIDGETS
total_wealth_tuple=('E160','K160','Y160','Y260','Y400','Y410')

#################################(10) BODP ############################################

## BODP
total_bodp_tuple=('D580',)

#################################(11) NET AUTHORIZED VOLUME #############################


#######PROFESSIONAL STUDENT LOC
net_prof_student_loc_tuple=('S021','S026','L290')


#######PROFESSIONAL: HEALTH CARE (PLACEHOLDER FOR NOW)
net_prof_health_tuple=('Z1234',)

####### GIC SBB Deposits
gic_sbb =('T008','T009','T043','T920','T921','T930','T943','T944','T990')

####### Business Essentials - Credit Card Increases
business_essentials_credit_card_increases =('V520','V975','V997','V999','V935','V955')




#################################(12) CONCATENATE ALL PRODUCT CODES TO FILTER RETAIL DATA LATER #############################

all_prod = (gross_all_tuple + total_cc_tuple + total_mer_tuple + total_cms_tuple + total_com_tuple + total_new_tuple + total_trade_tuple +
           total_bcp_tuple + total_mur_tuple + total_bodp_tuple + net_prof_student_loc_tuple + gic_sbb + business_essentials_credit_card_increases)



