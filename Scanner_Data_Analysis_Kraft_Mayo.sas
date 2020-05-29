data scanner_data_grocery_store(drop=COLUPC_char len_vend len_item);
infile 'H:\mayo_groc_1114_1165.csv' firstobs=2;
input IRI_KEY WEEK SY $ GE $ VEND $ ITEM $ UNITS DOLLARS  F $ D PR;
format COLUPC_char $13. COLUPC 13.;
len_item = length(ITEM);
len_vend = length(VEND);
if  len_vend < 5 then VEND = trim(repeat('0',5-len_vend-1)||VEND);
if  len_item < 5 then ITEM = trim(repeat('0',5-len_item-1)||ITEM);
COLUPC_char = compress(CATT(trim(SY),trim(GE),trim(VEND),trim(ITEM)));
COLUPC = COLUPC_char*1;
run;

data delivery_stores;
infile 'H:\Delivery_Stores' firstobs=2 expandtabs;
input IRI_KEY   	1-7
	  OU      	  $ 9-10
	  EST_ACV   	11-19
	  Market_Name $ 20-42
	  Open			42-49
	  Clsd 			50-54
	  MskdName    $ 55-63;
OU = strip(OU);
Market_Name = strip(Market_Name);
MskdName = compress(MskdName);
run;

data mayo_detail(keep= L4 L5 VOL_EQ PRODUCT_TYPE FLAVOR SUGAR_CONTENT PACKAGE FAT_CONTENT STYLE TYPE_MAYO COLUPC);
infile 'H:\prod_mayo.csv' firstobs=2 DLM= ',' DSD MISSOVER;
length L1 $33. L2 $17. L3 $28. L4 $28. L5 $29. L9 $32. SY $2. GE $1. VEND $5. ITEM $5.
	  UPC $17. specification $80. PRODUCT_TYPE $19. FLAVOR $12. SUGAR_CONTENT $20. PACKAGE $20. FAT_CONTENT $19. STYLE $20. TYPE_MAYO $20. ;
input L1 $ L2 $ L3 $ L4 $ L5 $ L9 $ 
	  Level UPC $ SY $ GE $ VEND $ ITEM $ 
	  specification $ VOL_EQ  PRODUCT_TYPE $ FLAVOR $ SUGAR_CONTENT $ PACKAGE $ FAT_CONTENT $ STYLE $ TYPE_MAYO $;

L1 = strip(L1);
L2 = strip(L2);
L3 = strip(L3);
L4 = strip(L4);
L5 = strip(L5);
L9 = strip(L9);
UPC = strip(UPC);
specification = strip(specification);
PRODUCT_TYPE = strip(PRODUCT_TYPE);
FLAVOR = strip(FLAVOR);
SUGAR_CONTENT = strip(SUGAR_CONTENT);
PACKAGE = strip(PACKAGE);
FAT_CONTENT = strip(FAT_CONTENT);
STYLE = strip(STYLE);
TYPE_MAYO = strip(TYPE_MAYO);

format COLUPC_char $13. COLUPC 13.;
len_item = length(ITEM);
len_vend = length(VEND);
if  len_vend < 5 then VEND = trim(repeat('0',5-len_vend-1)||VEND);
if  len_item < 5 then ITEM = trim(repeat('0',5-len_item-1)||ITEM);
COLUPC_char = compress(CATT(trim(SY),trim(GE),trim(VEND),trim(ITEM)));
COLUPC = COLUPC_char*1;
run;


proc print data = mayo_detail(obs=10);run;

proc sort data=Delivery_stores dupout = dups nodupkey;
by IRI_KEY;
run;

proc sql;
create table stores_details as
select * from Delivery_stores
where IRI_KEY not in (select distinct IRI_KEY from dups)
order by IRI_KEY;
quit;


data scanner_data;
set Scanner_data_grocery_store; 
run;
proc print data= scanner_data(obs=10);run;

proc print data = mayo_detail(obs=10);run;

proc sql;
create table sales_data  as
select
a.IRI_KEY, a.WEEK, a.UNITS, a.DOLLARS, a.F, a.D, a.PR,
b.*,
c.OU, c.Market_Name, c.MskdName,

((a.DOLLARS/a.UNITS)/b.VOL_EQ) as DOLLARS_PER_MAYO,

case 
	when a.D in (1,2) then 1
	else 0 end as disp,

case
	when a.F not in ('NONE') then 1
	else 0 end as Feature,

case
	when b.L5 in ('KRAFT','KRAFT DELICIOUSLY LIGHT','KRAFT MAYO','KRAFT SANDWICH SPREAD','KRAFT MIRACLE WHIP') then 'KRAFT'
	when b.L5 in ('PRIVATE LABEL') then 'PRIVATE LABEL'
	when b.L5 in ('HELLMANNS','HELLMANNS BIG SQUEZE','HELLMANS JUST 2 GOOD','HELLMANS LIGHT') then 'HELLMANNS'
	else 'OTHER' end as brand

from scanner_data a
inner join mayo_detail b on a.colupc  = b.colupc
inner join stores_details c on a.IRI_KEY = c.IRI_KEY
where b.PRODUCT_TYPE = 'MAYONNAISE'
order by a.IRI_KEY, a.WEEK, b.L4, b.L5, b.COLUPC;
quit;
proc print data= sales_data(obs=10);run;

proc sql;
create table sales_data as
select a.*, b.tot_units
from sales_data a 
inner join (select IRI_KEY, WEEK, brand,sum(UNITS) as tot_units
			from sales_data
			group by IRI_KEY, WEEK, brand) b 
on a.IRI_KEY = b.IRI_KEY and a.WEEK = b.WEEK and a.brand = b.brand;
quit;

/*weighted variables*/

data sales_data;
retain IRI_KEY WEEK brand L4 L5 COLUPC DOLLARS_PER_MAYO wt_price_mayo units tot_units PR PR_wt D disp_wt F Feature Feature_wt;
set sales_data;
format PR_wt 4.2 disp_wt 4.2 Feature_wt 4.2 DOLLARS_PER_MAYO 4.2 wt_price_mayo 4.2;
wt_price_mayo  = DOLLARS_PER_MAYO*units/tot_units;
PR_wt = PR*units/tot_units;
disp_wt = disp*units/tot_units;
Feature_wt = Feature*units/tot_units;
run; 
proc print data = sales_data(obs=10);run;
proc sql;
create table sales_brandwise as
select IRI_KEY, WEEK, brand,
sum(wt_price_mayo) as tot_wt_brand_price,
sum(PR_wt) as tot_PR_wt, 
sum(disp_wt) as tot_disp_wt, 
sum(Feature_wt) as tot_Feature_wt
from sales_data
group by IRI_KEY, WEEK, brand
order by 1,2,3;
quit;

proc export 
  data=sales_data 
  dbms=csv 
  outfile="H:\Sales_Data.csv" 
  replace;
run;

proc sql;
SELECT * FROM sales_brandwise limit 10;run;
data brand1 brand2 brand3 brand4;
set sales_brandwise;
if brand = 'KRAFT' then output brand1;
else if brand = 'PRIVATE LABEL' then output brand2;
else if brand = 'HELLMANNS' then output brand3;
else output brand4;
run;


proc sql;
create table all_brand_wt_price as
select
a.IRI_KEY, a.WEEK,

a.tot_wt_brand_price as wt_price_brand1,
a.tot_PR_wt as PR_wt_brand1,
a.tot_disp_wt as disp_wt_brand1,
a.tot_Feature_wt as Feature_wt_brand1,

b.tot_wt_brand_price as wt_price_brand2,
b.tot_PR_wt as PR_wt_brand2,
b.tot_disp_wt as disp_wt_brand2,
b.tot_Feature_wt as Feature_wt_brand2,

c.tot_wt_brand_price as wt_price_brand3,
c.tot_PR_wt as PR_wt_brand3,
c.tot_disp_wt as disp_wt_brand3,
c.tot_Feature_wt as Feature_wt_brand3,

d.tot_wt_brand_price as wt_price_brand4,
d.tot_PR_wt as PR_wt_brand4,
d.tot_disp_wt as disp_wt_brand4,
d.tot_Feature_wt as Feature_wt_brand4

from brand1 a 
inner join brand2 b on a.IRI_KEY = b.IRI_KEY and a.week = b.week
inner join brand3 c on a.IRI_KEY = c.IRI_KEY and a.week = c.week
inner join brand4 d on a.IRI_KEY = d.IRI_KEY and a.week = d.week
order by a.IRI_KEY, a.WEEK;
quit;
proc print data = all_brand_wt_price(obs=10);run;
proc sql;
Select * from all_brand_wt_price; run;

%macro brands(brand,brand_num);
proc sql;
create table brand_&brand_num. as
select 
b.*,

b.wt_price_brand1*b.PR_wt_brand1 as price_PR1,
b.wt_price_brand2*b.PR_wt_brand1 as price_PR2,
b.wt_price_brand3*b.PR_wt_brand1 as price_PR3,
b.wt_price_brand4*b.PR_wt_brand1 as price_PR4,

b.wt_price_brand1*b.Feature_wt_brand1 as price_F1,
b.wt_price_brand2*b.Feature_wt_brand2 as price_F2,
b.wt_price_brand3*b.Feature_wt_brand3 as price_F3,
b.wt_price_brand4*b.Feature_wt_brand3 as price_F4,

b.PR_wt_brand1*b.Feature_wt_brand1 as PR_F1,
b.PR_wt_brand2*b.Feature_wt_brand2 as PR_F2,
b.PR_wt_brand3*b.Feature_wt_brand3 as PR_F3,
b.PR_wt_brand4*b.Feature_wt_brand3 as PR_F4,

case when a.tot_units is null then 0
else a.tot_units end as tot_units

from all_brand_wt_price b
inner join (select IRI_KEY, week, brand,sum(UNITS) as tot_units
			from sales_data
			where brand = &brand.
			group by IRI_KEY, week, brand ) a
on a.IRI_KEY = b.IRI_KEY and a.week = b.week
order by IRI_KEY, week;
quit;

proc panel data=brand_&brand_num.;
model tot_units =   wt_price_brand1 wt_price_brand2 wt_price_brand3 wt_price_brand4
					disp_wt_brand1 disp_wt_brand2 disp_wt_brand3 disp_wt_brand4
					Feature_wt_brand1 Feature_wt_brand2 Feature_wt_brand3 Feature_wt_brand4
					PR_wt_brand1 PR_wt_brand2 PR_wt_brand3 PR_wt_brand4

					price_PR1 price_PR2 price_PR3 price_PR4
					price_F1 price_F2 price_F3 price_F4
					PR_F1 PR_F2 PR_F3 PR_F4
				    / fixtwo vcomp=fb plots=none;
id IRI_KEY week;
run;

%mend;

ODS TAGSETS.EXCELXP
file='H:\Elasticity.xls'
STYLE=minimal
OPTIONS ( Orientation = 'landscape'
FitToPage = 'yes'
Pages_FitWidth = '1'
Pages_FitHeight = '100' );
/*ods html file="example.html" path="H:\Elasticity";*/
%brands('KRAFT',1);
%brands('PRIVATE LABEL',2);
%brands('HELLMANNS',3);
%brands('OTHER',4);
ods tagsets.excelxp close;


proc means data = brand_1;
var wt_price_brand1;
run;


proc means data = brand_2;
var wt_price_brand2;
run;

proc means data = brand_3;
var wt_price_brand3;
run;

proc means data = brand_4;
var wt_price_brand4;
run;
/* brand choice///////////////////////////////////////////// */
data panel_data_GR;
infile 'H:\mayo_PANEL_GR_1114_1165.dat' firstobs=2 expandtabs;
input PANID	WEEK UNITS OUTLET $ DOLLARS IRI_KEY COLUPC;
run;


data panel_details;
infile 'H:\Panel_Demo.csv' firstobs=2 DLM=',' DSD MISSOVER;
input PANID panel_churn pre_tax_inc fam_size HH_RACE Home_type COUNTY $ AGE EDU OCC 
	  Age_Male_head EDU_male_head OCC_male_head Male_work_hr Male_smoke 
	  Age_Female_head EDU_Female_head OCC_Female_head Female_work_hr Female_smoke
	  Dogs Cats Children_age_grp Marital_Status Language TV Cable_TV Year HISP_FLAG
      HISP_CAT Race2 RACE3 Microwave ZIPCODE FIPSCODE market_zipcode 
	  IRI_Geo_num EXT_FACT;
run;

data all_panel_data;
set 
	panel_data_GR
	;
run;

proc sql;
create table panel_mayo_details as
select
a.*,
b.vol_eq,
b.L4,
case
	when b.L5 in ('KRAFT','KRAFT DELICIOUSLY LIGHT','KRAFT MAYO','KRAFT SANDWICH SPREAD','KRAFT MIRACLE WHIP') then 'KRAFT'
	when b.L5 in ('PRIVATE LABEL') then 'PRIVATE LABEL'
	when b.L5 in ('HELLMANNS','HELLMANNS BIG SQUEZE','HELLMANS JUST 2 GOOD','HELLMANS LIGHT') then 'HELLMANNS'
	else 'OTHER' end as brand

from all_panel_data a
inner join mayo_detail b on a.COLUPC = b.COLUPC
where b.PRODUCT_TYPE = 'MAYONNAISE';
quit;
proc export 
  data=mayo_detail
  dbms=csv 
  outfile="H:\mayo_detail.csv" 
  replace;
run;
proc sql;
Select * from panel_mayo_details;run;
proc sql;
SELECT * FROM all_brand_wt_price;run;

proc export 
  data=panel_mayo_details
  dbms=csv 
  outfile="H:\Panel_mayo_details.csv" 
  replace;
run;

proc export 
  data=all_brand_wt_price 
  dbms=csv 
  outfile="H:\all_brand_wt_price.csv" 
  replace;
run;
proc sql;
drop table brand_choice_store_features;run;
proc sql;
create table brand_choice_store_features as
select
a.*,
b.*
from panel_mayo_details a
inner join all_brand_wt_price b 
on a.WEEK = b.WEEK
order by a.PANID, a.WEEK;
quit;
proc sql;
Select * from brand_choice_store_features;run;

proc export 
  data=brand_choice_store_features
  dbms=csv 
  outfile="H:\brand_choice.csv" 
  replace;
run;
data brand_choice_store_features;
set brand_choice_store_features(drop= outlet dollars iri_key colupc L4);
if brand = 'KRAFT' then brand_id = 1;
else if brand = 'PRIVATE LABEL' then brand_id = 2;
else if brand = 'HELLMANNS' then brand_id = 3;
else brand_id = 4;
run;

ods html close;
ods html;

proc logistic data=brand_choice_store_features; 
class brand (ref= 'KRAFT');
model brand = 
/*wt_price_brand1 wt_price_brand2 wt_price_brand3 wt_price_brand4*/
				disp_wt_brand1 disp_wt_brand2 disp_wt_brand3 disp_wt_brand4 
				Feature_wt_brand1 Feature_wt_brand2 Feature_wt_brand3 Feature_wt_brand4
				PR_wt_brand1 PR_wt_brand2 PR_wt_brand3 PR_wt_brand4
				/ link = glogit expb clodds=PL; 
run; 




data cust_demographic;
set panel_details(keep=PANID pre_tax_inc fam_size Home_type Age_Male_head EDU_male_head OCC_male_head Male_work_hr  Age_Female_head EDU_Female_head OCC_Female_head Female_work_hr Dogs Cats Children_age_grp Marital_Status TV Cable_TV);
RUN;

proc sql;
create table cust_desc as
select
b.panid, 
case
	when b.fam_size in (4,5,6) then 'large'
	when b.fam_size in (1,2,3) then 'regular'
	else 'other' end as fam_size,

case
	when b.pre_tax_inc in (1,2,3,4) then 'low'
	when b.pre_tax_inc in (5,6,7,8) then 'medium'
	when b.pre_tax_inc in (9,10,11) then 'high'
	when b.pre_tax_inc in (12) then 'very_high'
	else 'other' end as fam_income,

case
	when b.age_male_head in (1) then 'young'
	when b.age_male_head in (2,3,4) then 'mid_career'
	when b.age_male_head in (5,6) then 'elder'
	else 'other' end as age_male,

case
	when b.age_Female_head in (1) then 'young'
	when b.age_Female_head in (2,3,4) then 'mid_career'
	when b.age_Female_head in (5,6) then 'elder'
	else 'other' end as age_female,

case
	when b.edu_male_head in (1,2,3) then 'school'
	when b.edu_male_head in (4,5,6) then 'college'
	when b.edu_male_head in (7,8) then 'graduate'
	else 'other' end as educ_male,

case
	when b.edu_Female_head in (1,2,3) then 'school'
	when b.edu_Female_head in (4,5,6) then 'college'
	when b.edu_Female_head in (7,8) then 'graduate'
	else 'other' end as educ_female,
case
	when b.Children_age_grp in (1,2,3) then 1
	when b.Children_age_grp in (4,5,6) then 2
	when b.Children_age_grp in (7) then 3
	else 0 end as child_num,
case 
	when b.OCC_male_head in (1,2,3) then 'white_high'
	when b.OCC_male_head in (4,5,6) then 'white_low'
	when b.OCC_male_head in (7,8,9) then 'blue'
	when b.OCC_male_head in (10,11,12) then 'no_occup'
	else 'other' end as occu_male,
case 
	when b.OCC_Female_head in (1,2,3) then 'white_high'
	when b.OCC_Female_head in (4,5,6) then 'white_low'
	when b.OCC_Female_head in (7,8,9) then 'blue'
	when b.OCC_Female_head in (10,11,12) then 'no_occup'
	else 'other' end as occu_female,
cats+dogs as pets_total
from panel_details b 
order by b.panid;
quit;

/* Variables for Demographic Study */
data cust_desc2;
set cust_desc;
IF fam_size='large' THEN fam_size_L=1 ; ELSE fam_size_L=0;
IF fam_size='regular' THEN fam_size_R=1 ; ELSE fam_size_R=0;
IF fam_size='other' THEN fam_size_O=1 ; ELSE fam_size_O=0;
IF fam_income="low" THEN fam_income_L=1 ; ELSE fam_income_L=0;
IF fam_income="medium" THEN fam_income_M=1 ; ELSE fam_income_M=0;
IF fam_income="high" THEN fam_income_H=1 ; ELSE fam_income_H=0;
IF fam_income="very_high" THEN fam_income_VH=1 ; ELSE fam_income_VH=0;
IF fam_income="other" THEN fam_income_O=1 ; ELSE fam_income_O=0;
IF age_male="young" THEN age_mY=1 ; ELSE age_mY=0;
IF age_male="mid_career" THEN age_mM=1 ; ELSE age_mM=0;
IF age_male="elder" THEN age_mE=1 ; ELSE age_mE=0;
IF age_male="other" THEN age_mO=1 ; ELSE age_mO=0;
IF age_female="young" THEN age_fY=1 ; ELSE age_fY=0;
IF age_female="mid_career" THEN age_fM=1 ; ELSE age_fM=0;
IF age_female="elder" THEN age_fE=1 ; ELSE age_fE=0;
IF age_female="other" THEN age_fO=1 ; ELSE age_fO=0;
IF educ_male="school" THEN educ_mS=1 ; ELSE educ_mS=0;
IF educ_male="college" THEN educ_mC=1 ; ELSE educ_mC=0;
IF educ_male="other" THEN educ_mO=1 ; ELSE educ_mO=0;
IF educ_male="graduate" THEN educ_mG=1 ; ELSE educ_mG=0;

IF educ_female="school" THEN educ_fS=1 ; ELSE educ_fS=0;
IF educ_female="college" THEN educ_fC=1 ; ELSE educ_fC=0;
IF educ_female="graduate" THEN educ_mG=1 ; ELSE educ_mG=0;
IF educ_female="other" THEN educ_fO=1 ; ELSE educ_fO=0;
IF occu_male="white_high" THEN occ_mWH=1; ELSE occ_mWH=0;
IF occu_male="white_low" THEN occ_mWL=1; ELSE occ_mWL=0;
IF occu_male="blue" THEN occ_mB=1; ELSE occ_mB=0;
IF occu_male="no_occup" THEN occ_mNO=1; ELSE occ_mNO=0;

IF occu_female="white_high" THEN occ_fWH=1; ELSE occ_fWH=0;
IF occu_female="white_low" THEN occ_fWL=1; ELSE occ_fWL=0;
IF occu_female="blue" THEN occ_fB=1; ELSE occ_fB=0;
IF occu_female="no_occup" THEN occ_fNO=1; ELSE occ_fNO=0;
IF child_num=1 THEN one_child=1; ELSE one_child=0;
IF child_num=2 THEN two_child=1; ELSE two_child=0;
IF child_num=3 THEN three_child=1; ELSE three_child=0;
IF child_num=0 THEN zero_child=1; ELSE zero_child=0;
RUN;

ods html file="multilogit.html" path="H:\";
proc logistic data=brand_choice_store_features; 
class brand (ref= 'KRAFT');
model brand = 
/*wt_price_brand1 wt_price_brand2 wt_price_brand3 wt_price_brand4*/
				disp_wt_brand1 disp_wt_brand2 disp_wt_brand3 disp_wt_brand4
				Feature_wt_brand1 Feature_wt_brand2 Feature_wt_brand3 Feature_wt_brand4
				PR_wt_brand1 PR_wt_brand2 PR_wt_brand3 PR_wt_brand4
				/ link = glogit expb clodds=PL; 
run; 
ods html close;

proc print data = cust_desc2(obs=10);run;

proc sql;
create table demo as
select
a.*,c.*
from brand_choice_store_features a
inner join cust_desc2 c on a.PANID = c.PANID;
proc export 
  data=demo
  dbms=csv 
  outfile="H:\demo.csv" 
  replace;
run;

proc print data = demo(obs=10);run;
ods html file="logit.html" path="H:\";
proc logistic data=demo; 
class fam_size fam_income age_male age_female educ_male educ_female child_num occu_male occu_female brand(ref= 'KRAFT');
model brand = fam_size*brand fam_income*brand age_male*brand age_female*brand educ_male*brand educ_female*brand child_num*brand occu_male*brand occu_female*brand / link = glogit expb clodds=PL;
quit;
ods html close;
proc freq data = demo;table brand;run;

ods html file="catmod.html" path="H:\";
proc catmod data = demo order = data;
  response LOGITS;
  model brand = fam_size fam_income age_male age_female educ_male educ_female child_num occu_male occu_female;
run;
ods html close;

proc print data = demo (obs=10);run;

proc export 
  data=demo 
  dbms=xlsx 
  outfile="H:\Demo.xlsx" 
  replace;
run;
proc import 
  datafile="H:\Project\Data\Demo.xlsx" 
  dbms=xlsx 
  out=work.demo1 
  replace;
run;


ods listing;

ods results;
proc freq data = cust_desc;table child_num;run; 

