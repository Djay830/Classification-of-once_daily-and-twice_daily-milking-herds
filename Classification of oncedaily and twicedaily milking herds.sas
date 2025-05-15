* Herd Test;

*Herd_test_data for classification of OAD and TAD;

proc freq data=Sort_herdtest_data2017;
tables sample_regime_code sample_regime_descr Number_of_HTs owner_ptpt_code;
run;quit;

proc freq data=Sort_herdtest_data2017;
tables map_reference*sample_regime_descr;
run;quit;

proc sort data=Sort_herdtest_data2017 out=Sort_herdtest_data2017;
by lic_animal_key herd_test_date;
run;quit;

proc sql;
 create table herd_test1 as
    select distinct owner_ptpt_code,map_reference, herd_test_date, sample_regime_descr, count(*) as nr
from sort_herdtest_data2017
group by owner_ptpt_code, herd_test_date, sample_regime_descr
order by owner_ptpt_code, herd_test_date;
quit;

proc sql;
 create table herd_test2 as
    select distinct owner_ptpt_code,map_reference, herd_test_date, sample_regime_descr, count(*) as total
from sort_herdtest_data2017
group by owner_ptpt_code, herd_test_date
order by owner_ptpt_code, herd_test_date;
quit;

proc sql;
create table herd_percent as
select distinct x.*,y.total
from herd_test1 x left join herd_test2 y
on (x.owner_ptpt_code=y.owner_ptpt_code)and (x. herd_test_date=y. herd_test_date)
order by owner_ptpt_code;
run;

proc sql;
create table herd_percent as
select distinct x.*,y.total
from herd_test1 x left join herd_test2 y
on (x.owner_ptpt_code=y.owner_ptpt_code)and (x. herd_test_date=y. herd_test_date)
order by owner_ptpt_code;
quit;

proc sql;
 create table herd_percent as
    select distinct owner_ptpt_code,map_reference,herd_test_date,sample_regime_descr,nr,total, max(total) as hs
from herd_percent
group by owner_ptpt_code
order by owner_ptpt_code;
quit;

data herd_percent;
set herd_percent;
percent=int((nr/total)*100);
run;

proc sql;
 create table maxpercent as
    select distinct owner_ptpt_code,map_reference, herd_test_date,max(percent) as percent
from herd_percent
group by owner_ptpt_code, herd_test_date
order by owner_ptpt_code, herd_test_date;
quit;

proc sql;
create table maxpercent as
select distinct x.*,y. sample_regime_descr, y.nr, y.total, y.hs
from maxpercent x left join herd_percent y
on (x.owner_ptpt_code=y.owner_ptpt_code)and (x. herd_test_date=y. herd_test_date)and (x. percent= y. percent) and (x. map_reference=y. map_reference)
order by owner_ptpt_code;
quit;

data maxpercent;
 set maxpercent;
 by owner_ptpt_code herd_test_date;
 ht2=lag(herd_test_date);
 if first.owner_ptpt_code then ht2=.;
 format ht2 ddmmyy8.;
 ht_int=herd_test_date-ht2;
run; quit;


data maxpercent;
set maxpercent;
if sample_regime_descr='OAD' then code_sample_regime=1;
else if sample_regime_descr='PM & AM' then code_sample_regime=2;
else if sample_regime_descr='PM' then code_sample_regime=2;
else if sample_regime_descr='AM' then code_sample_regime=2;
run;quit;


*To remove the non seasonal herds;
proc sql;
 create table maxpercent as
    select distinct x.*, y. ffr_mating_start_date, y. ffr_mating_end_date, y. calving_system, y. analysis_level
    from maxpercent x left join Sort_repro_data2017 y
    on (x.owner_ptpt_code=y.owner_ptpt_code)
    order by owner_ptpt_code;
run; quit;

data maxpercent;
set maxpercent;
if calving_system='Seasonal' then calving_system='Seasonal';
else delete;
run;

proc sql;
 create table maxpercent1 as
    select distinct owner_ptpt_code, herd_test_date, ht_int,percent,total,hs,ncows2, sample_regime_descr 
    from maxpercent 
	where ht_int in (1,2,3) and percent>10
	group by owner_ptpt_code
    order by owner_ptpt_code;
quit;

proc sort data=maxpercent1 nodupkey out=maxpercent1;
by owner_ptpt_code;
run;


*number of cows in calving, mating, PD;
proc sql;
create table calving_mating as
select distinct x.owner_ptpt_code, x.map_reference,x.lic_animal_key,x.birth_id,x.partn_date,y.mating_date 
from Removedup_calving_data_2017 x left join Sortmatingdate_2017 y
on (x.owner_ptpt_code=y.owner_ptpt_code)and (x.map_reference=y.map_reference) and (x.lic_animal_key=y.lic_animal_key) and (x. birth_id=y. birth_id)
order by owner_ptpt_code, lic_animal_key;
quit;

proc sql;
create table calving_mating_PD as
select distinct x. *,y. reprod_status_descr , y. pregnancy_days_count  
from calving_mating x left join Sort_pd_data_2017 y
on (x.owner_ptpt_code=y.owner_ptpt_code)and (x.lic_animal_key=y.lic_animal_key) and (x. birth_id=y. birth_id)
order by owner_ptpt_code, lic_animal_key;
quit;

proc sort data=calving_mating_PD nodupkey out=calving_mating_PD;
by lic_animal_key;
run;

proc sql;
 create table ncows2 as
    select distinct owner_ptpt_code,map_reference,lic_animal_key, count(lic_animal_key) as ncows2
from calving_mating_PD 
group by owner_ptpt_code
order by owner_ptpt_code, lic_animal_key;
quit;
proc sort data=ncows2 nodupkey out=ncows2;
by owner_ptpt_code;
run;

proc sql;
create table maxpercent as
select distinct x.*,y. ncows2
from maxpercent x left join ncows2 y
on (x.owner_ptpt_code=y.owner_ptpt_code)
order by owner_ptpt_code;
quit;

data maxpercent;
set maxpercent;
correct_per= int((nr/ncows2)*100); *all herds have atleast  50 cows;
run;quit;



**** Transpose;;;;

proc transpose data=maxpercent prefix=herd_test_date  out=transpose_htdate;
 by owner_ptpt_code;
 var herd_test_date;
run; quit;

proc transpose data=maxpercent prefix=code_sample_regime out=transpose_mreg;
 by owner_ptpt_code ;
 var code_sample_regime ;
 run; quit;

proc transpose data=maxpercent prefix=percent  out=transpose_percent;
 by owner_ptpt_code;
 var percent;
run; quit;

proc transpose data=maxpercent prefix=correct_per  out=transpose_correct_per;
 by owner_ptpt_code;
 var percent;
run; quit;

proc sql;
 create table herds as
    select distinct x.*, y.*
    from transpose_htdate x left join transpose_mreg y
    on (x.owner_ptpt_code=y.owner_ptpt_code)
    order by owner_ptpt_code;
run; quit;

proc sql;
 create table herds as
    select distinct x.*, y.*
    from herds x left join transpose_percent y
    on (x.owner_ptpt_code=y.owner_ptpt_code)
    order by owner_ptpt_code;
run; quit;

proc sql;
 create table herds as
    select distinct x.*, y.*
    from herds x left join transpose_correct_per y
    on (x.owner_ptpt_code=y.owner_ptpt_code)
    order by owner_ptpt_code;
run; quit;

*mcd;
proc sql;
 create table mcd as
    select distinct owner_ptpt_code, median(partn_date)as median_calving_date format=date9.
from Removedup_calving_data_2017
group by owner_ptpt_code 
order by owner_ptpt_code;
quit;

proc sql;
 create table herds as
    select distinct x.*, y. median_calving_date
    from herds x left join mcd y
    on (x.owner_ptpt_code=y.owner_ptpt_code)
    order by owner_ptpt_code;
run; quit;

proc sql;
 create table herds as
    select distinct x.*, y. ffr_mating_start_date, y. ffr_mating_end_date, y. analysis_level
    from herds x left join Sort_repro_data2017 y
    on (x.owner_ptpt_code=y.owner_ptpt_code)
    order by owner_ptpt_code;
run; quit;

*dim from median calving to herd test date;
%macro m1;
    %do n = 1 %to 40;
         data herds;
         set herds;
         dim&n = datdif(median_calving_date,herd_test_date&n,'Actual');
    %end;
%mend m1;
%m1
proc means data=herds;
var dim1--dim40;
run;

data herds;
set herds;
if '05MAR2018'd<= median_calving_date <= '04MAY2018'd then delete; *here outlier herds 29 removed;
run;

*Again combined the map_reference into herds table;
proc sql;
create table herds as
select distinct x.*,y. map_reference, y.ncows2
from herds x left join maxpercent y
on (x.owner_ptpt_code=y.owner_ptpt_code)
order by owner_ptpt_code;
quit;

data herds;
set herds;
if analysis_level='Basic' then delete;
run;quit;


*To classify the totally OAD,TAD and mixed;
data herds;
 set herds;
 array regime(40) code_sample_regime1--code_sample_regime40;
 nh=0;
 tsum=0;
 do i = 1 to 40;
  if regime(i)^=. then do; nh=nh+1; tsum=tsum+regime(i); end;
 end;
 drop i;
run; quit;

proc sql;
alter table herds
  modify mfrq char(20) format=$20.;
quit;

data herds;
 set herds;
 mfrq='Mixed';
 if tsum=2*nh then mfrq='Whole_TAD';
 if tsum=nh   then mfrq='Whole_OAD';
run; quit;
 
proc freq data=herds;
 table mfrq;
run; quit;


* more than 90% in totally TAD cows;*1369 herds ;

proc sql;
 create table TAD as
    select distinct owner_ptpt_code,map_reference,mfrq 
    from herds
	where mfrq='Whole_TAD'
	order by owner_ptpt_code;
run; quit;

proc sql;
 create table TAD90 as
    select distinct x.*, y. mfrq
    from maxpercent x left join TAD y
    on (x.owner_ptpt_code=y.owner_ptpt_code)and (x.map_reference=y.map_reference)
    order by owner_ptpt_code;
run; quit;

data TAD90;
set TAD90;
if mfrq=''  then delete;
run;quit;

data TAD90;
set TAD90;
if percent>=90 then regime1=2;
else regime1=0;
run;quit;

proc sql;
 create table TAD90_herds as
    select distinct owner_ptpt_code,map_reference,herd_test_date, percent,correct_per,regime1,count (herd_test_date) as nht
    from TAD90
	group by owner_ptpt_code
    order by owner_ptpt_code;
run; quit;

proc sql;
 create table TAD90_herds1 as
    select distinct owner_ptpt_code,map_reference,herd_test_date, percent,correct_per,nht,regime1, count(regime1) as nTAD 
    from TAD90_herds
	where regime1=2
	group by owner_ptpt_code
    order by owner_ptpt_code;
run; quit;

data TAD90_herds1;
set TAD90_herds1;
if nht^=nTAD then delete;
run;quit;

proc sort data=TAD90_herds1 nodupkey out=TAD90_herds1;*from 1762 only 1369 herds have the more than 90% cows in TAD milking;
by owner_ptpt_code;
run;

proc sql;
 create table herds as
    select distinct x.*, y. regime1
    from herds x left join TAD90_herds1 y
    on (x.owner_ptpt_code=y.owner_ptpt_code)and (x.map_reference=y.map_reference)
    order by owner_ptpt_code;
quit;

data herds;
set herds;
if regime1=2 then new_mf1='Total_TAD';
run;quit;

proc freq data=herds;
tables new_mf1;
run;quit;

proc sql;
alter table herds
  modify new_mf1 char(50) format=$50.;
quit;


* more than 90% in totally OAD cows;*262 herds;

proc sql;
 create table OAD as
    select distinct owner_ptpt_code,map_reference,mfrq 
    from herds
	where mfrq='Whole_OAD'
	order by owner_ptpt_code;
run; quit;

proc sql;
 create table OAD90 as
    select distinct x.*, y. mfrq
    from maxpercent x left join OAD y
    on (x.owner_ptpt_code=y.owner_ptpt_code)and (x.map_reference=y.map_reference)
    order by owner_ptpt_code;
run; quit;

data OAD90;
set OAD90;
if mfrq=''  then delete;
run;quit;

data OAD90;
set OAD90;
if percent>=90 then regime=1;
else regime=0;
run;quit;

proc sql;
 create table OAD90_herds as
    select distinct owner_ptpt_code,map_reference,herd_test_date, percent,correct_per,regime,count (herd_test_date) as nht
    from OAD90
	group by owner_ptpt_code
    order by owner_ptpt_code;
run; quit;

proc sql;
 create table OAD90_herds1 as
    select distinct owner_ptpt_code,map_reference,herd_test_date, percent,correct_per,nht,regime, count(regime) as nOAD 
    from OAD90_herds
	where regime=1
	group by owner_ptpt_code
    order by owner_ptpt_code;
run; quit;

data OAD90_herds1;
set OAD90_herds1;
if nht^=nOAD then delete;
run;quit;

proc sort data=OAD90_herds1 nodupkey out=OAD90_herds1;*from 267 only 262 herds have the more than 90% cows in OAD milking;
by owner_ptpt_code;
run;

proc sql;
 create table herds as
    select distinct x.*, y. regime
    from herds x left join OAD90_herds1 y
    on (x.owner_ptpt_code=y.owner_ptpt_code)and (x.map_reference=y.map_reference)
    order by owner_ptpt_code;
quit;

data herds;
set herds;
if regime=1 then new_mf1='Total_OAD';
run;quit;

proc freq data=herds;
tables new_mf1;
run;quit;


*categorizing mix herds further;

data mix; *Totally 1724 herds in mixed category;
set herds;
if mfrq='Whole_TAD' then delete;
else if mfrq='Whole_OAD' then delete;
run;quit;

*TAD upto 31st december and then OAD;*895 herds;
data mix;
 set mix;
 array frequency(40) code_sample_regime1--code_sample_regime40;
 array htdate(40) herd_test_date1--herd_test_date40;
 nh2=0;
 tsum2=0;
 do i = 1 to 40;
  if frequency(i)^=. and htdate(i)<='31DEC2017'd then do; nh2=nh2+1; tsum2=tsum2+frequency(i); end;
 end;
 drop i;
run; quit;

data mix;
set mix;
if tsum2=2*nh2 then category=1;
else category=2;
run;quit;

proc sql;
alter table mix
  modify new_mf1 char(50) format=$50.;
quit;

 data mix;
 set mix;
 nh3=nh-nh2;
 tsum3=tsum-tsum2;
 if nh3=tsum3 and category=1 then new_mf1='TAD(up_dec31)_after_OAD';
 if tsum = ((2*nh)-1) and category=1 then new_mf1='OAD_at_end'; *but here some herds do not have the OAD at end, upto december TAD thenafter OAD and again TAD, this algorithem didnt work for those data;
 run;quit;

proc freq data=mix;
tables new_mf1;
run;quit;

* 749 herds(>90%) have TAD up to december 31 and then after OAD in whole season;
 
data TAD_after_OAD;
set mix;
if new_mf1='OAD_at_end' then delete;
if new_mf1='' then delete;
run;quit;


proc sql;
 create table TAD_after_OAD90 as
    select distinct x.*, y. new_mf1
    from maxpercent x left join TAD_after_OAD y
    on (x.owner_ptpt_code=y.owner_ptpt_code)and (x.map_reference=y.map_reference)
    order by owner_ptpt_code;
run; quit;

data TAD_after_OAD90;
set TAD_after_OAD90;
if new_mf1=''  then delete;
run;quit;

data TAD_after_OAD90;
set TAD_after_OAD90;
if percent>=90 then regime2=3;
else regime2=0;
run;quit;

proc sql;
 create table TAD_after_OAD90_new as
    select distinct owner_ptpt_code,map_reference,herd_test_date, percent,correct_per,regime2,count (herd_test_date) as nht
    from TAD_after_OAD90
	group by owner_ptpt_code
    order by owner_ptpt_code;
run; quit;

proc sql;
 create table TAD_after_OAD90_new as
    select distinct owner_ptpt_code,map_reference,herd_test_date, percent,correct_per,nht,regime2, count(regime2) as nTAD_OAD 
    from TAD_after_OAD90_new
	where regime2=3
	group by owner_ptpt_code
    order by owner_ptpt_code;
run; quit;

data TAD_after_OAD90_new;
set TAD_after_OAD90_new;
if nht^=nTAD_OAD then delete;
run;quit;

proc sort data=TAD_after_OAD90_new nodupkey out=TAD_after_OAD90_new;*749 herds have the more than 90% cows in TAD milking before 31st december and after OAD;
by owner_ptpt_code;
run;

proc sql;
 create table herds as
    select distinct x.*, y. regime2
    from herds x left join TAD_after_OAD90_new y
    on (x.owner_ptpt_code=y.owner_ptpt_code)and (x.map_reference=y.map_reference)
    order by owner_ptpt_code;
quit;

data herds;
set herds;
if regime2=3 then new_mf1='TAD(up_dec31)_after_OAD';
run;quit;

proc freq data=herds;
tables new_mf1;
run;quit;


* 102 herds(90% cows) OAD at end HT;

data OAD_end;
set mix;
if new_mf1='TAD(up_dec31)_after_OAD' then delete;
if new_mf1='' then delete;
run;quit;

proc sql;
 create table OAD90_atend as
    select distinct x.*, y. new_mf1
    from maxpercent x left join OAD_end y
    on (x.owner_ptpt_code=y.owner_ptpt_code)and (x.map_reference=y.map_reference)
    order by owner_ptpt_code;
run; quit;

data OAD90_atend;
set OAD90_atend;
if new_mf1=''  then delete;
run;quit;

data OAD90_atend;
set OAD90_atend;
if percent>=90 then regime3=4;
else regime3=0;
run;quit;

proc sql;
 create table OAD90_atend_new as
    select distinct owner_ptpt_code,map_reference,herd_test_date, percent,correct_per,regime3,count (herd_test_date) as nht
    from OAD90_atend
	group by owner_ptpt_code
    order by owner_ptpt_code;
run; quit;

proc sql;
 create table OAD90_atend_new as
    select distinct owner_ptpt_code,map_reference,herd_test_date, percent,correct_per,nht,regime3, count(regime3) as nTAD_OAD_end 
    from OAD90_atend_new
	where regime3=4
	group by owner_ptpt_code
    order by owner_ptpt_code;
run; quit;

data OAD90_atend_new;
set OAD90_atend_new;
if nht^=nTAD_OAD_end  then delete;
run;quit;

proc sort data=OAD90_atend_new nodupkey out=OAD90_atend_new;* 109 herds have OAD at end of the herd test;
by owner_ptpt_code;
run;

proc sql;
 create table herds as
    select distinct x.*, y. regime3
    from herds x left join OAD90_atend_new y
    on (x.owner_ptpt_code=y.owner_ptpt_code)and (x.map_reference=y.map_reference)
    order by owner_ptpt_code;
quit;

data herds;
set herds;
if regime3=4 then new_mf1='OAD_in_lastHT';
run;quit;

proc freq data=herds;*371 herds had the TAD and thenafter OAd in last HT;
tables new_mf1;
run;quit;

proc sql;
 create table OAD_in_lastHT as
    select distinct owner_ptpt_code,map_reference,new_mf1,herd_test_date1, herd_test_date2, herd_test_date3, herd_test_date4, herd_test_date5,code_sample_regime1,code_sample_regime2,code_sample_regime3,code_sample_regime4,code_sample_regime5 
    from herds 
	where new_mf1='OAD_in_lastHT'
    order by owner_ptpt_code;
quit;

*Need to delete herds, those have OAD in middle then TAD;*finally 343 herds;
data herds;
set herds;
if new_mf1='OAD_in_lastHT' and code_sample_regime3=1 and code_sample_regime4=2 then delete;
if new_mf1='OAD_in_lastHT' and code_sample_regime4=1 and code_sample_regime5=2 then delete;
run;quit;

proc freq data=herds;
tables new_mf1;
run;quit;


*95 OAD  milking in around mating;
data mix;
 set mix;
 array frequency(40) code_sample_regime1--code_sample_regime40;
 array htdate(40) herd_test_date1--herd_test_date40;
 nh4=0;
 tsum4=0;
 if category=2 then 
 do i = 1 to 40;
  if frequency(i)^=. and htdate(i)<=ffr_mating_end_date then do; nh4=nh4+1; tsum4=tsum4+frequency(i); end;
 end;
 drop i;
run; quit;

data mix;
 set mix;
 if category=2 and nh4=tsum4 then new_mf1='OAD_mating';
 if category=2 and tsum4=(2*nh4-1) then new_mf1='OAD_mating';
 run;quit;

data OAD_mating;
set mix;
if new_mf1 in ('TAD(up_dec31)_after_OAD','OAD_at_end','' ) then delete; *Total 133 herds have OAD around mating;
else regime4=0;
run;quit;


proc sql;
 create table OAD90_mating as
    select distinct x.*, y. new_mf1 
    from maxpercent x left join OAD_mating y
    on (x.owner_ptpt_code=y.owner_ptpt_code)and (x.map_reference=y.map_reference)
    order by owner_ptpt_code;
run; quit;


data OAD90_mating;
set OAD90_mating;
if new_mf1=''  then delete;
run;quit;

data OAD90_mating;
set OAD90_mating;
if percent>=90 then regime4=5;
else regime4=0;
run;quit;

proc sql;
 create table OAD_mating_new as
    select distinct owner_ptpt_code,map_reference,herd_test_date, percent,correct_per,regime4,count (herd_test_date) as nht
    from OAD90_mating
	group by owner_ptpt_code
    order by owner_ptpt_code;
run; quit;

proc sql;
 create table OAD_mating_new as
    select distinct owner_ptpt_code,map_reference,herd_test_date, percent,correct_per,nht,regime4, count(regime4) as OAD_mating 
    from OAD_mating_new
	where regime4=5
	group by owner_ptpt_code
    order by owner_ptpt_code;
run; quit;

data OAD_mating_new;
set OAD_mating_new;
if nht^=OAD_mating   then delete;
run;quit;

proc sort data=OAD_mating_new nodupkey out=OAD_mating_new;* 95 herds have OAD milking around mating,14-detailed and 7-intermediate;
by owner_ptpt_code;
run;

proc sql;
 create table herds as
    select distinct x.*, y. regime4
    from herds x left join OAD_mating_new y
    on (x.owner_ptpt_code=y.owner_ptpt_code)and (x.map_reference=y.map_reference)
    order by owner_ptpt_code;
quit;

data herds;
set herds;
if regime4=5 then new_mf1='OAD_in_mating';
run;quit;

*Summary of herds,1369_TAD,262-OAD,95-OAD in mating, 749-TAD(up_dec31)_after_OAD, 262-OAD_in_lastHT;
proc freq data=herds;
tables new_mf1;
run;quit;


proc freq data=herds;
tables analysis_level*new_mf1;
run;quit;

