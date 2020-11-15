import psycopg2


def set_tab_hf_id():
    """
提取心衰病人hadm_id
    """
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_hf_id;"
                "create table tab_hf_id as "
                "select distinct hadm_id "
                "from mimiciii.diagnoses_icd "
                "where cast(seq_num as integer)<3 "
                "and icd9_code in "
                "(select icd9_code from mimiciii.d_icd_diagnoses "
                "where long_title like '%heart failure%' and long_title not like '%without%');")
    conn.commit()
    cur.close()
    conn.close()


def set_tab_patient_0():
    """
提取病人的subject_id/hadm_id/住院天数/性别/年龄
    """
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_patient_0;"
                "create table tab_patient_0 as "
                "select admissions.subject_id,admissions.hadm_id,patients.gender,"
                "cast((date(dischtime)-date(admittime)) as integer) as hosp_day,"
                "cast(extract(year from age(date(admissions.admittime),date(patients.dob))) as integer) as p_age "
                "from mimiciii.admissions left join mimiciii.patients "
                "on admissions.subject_id = patients.subject_id;")
    conn.commit()
    cur.close()
    conn.close()


def set_tab_patient_end():
    """
提取心衰病人的subject_id/hadm_id/住院天数/性别/年龄
    """
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_patient_end;"
                "create table tab_patient_end as "
                "select tab_patient_0.subject_id,tab_patient_0.hadm_id,tab_patient_0.gender,tab_patient_0.p_age,tab_patient_0.hosp_day "
                "from public.tab_hf_id left join public.tab_patient_0 "
                "on tab_hf_id.hadm_id = tab_patient_0.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()


def set_tab_HW_0():
    """
提取心衰病人的身高/体重
    """
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_HW_0;"
                "create table tab_HW_0 as "
                "with ch0 as "
                "(select hadm_id,itemid,valuenum from mimiciii.chartevents "
                "where hadm_id in (select hadm_id from tab_hf_id) "
                "and (itemid='226512' or itemid='762' or itemid='763' "
                "or itemid='226707' or itemid='920' or itemid='1394' or itemid='3486' or itemid='226730' or itemid='4188') "
                "order by hadm_id)"
                "select ch0.hadm_id,ch0.itemid,ch0.valuenum "
                "from ch0 join mimiciii.admissions "
                "on ch0.hadm_id = admissions.hadm_id "
                "order by ch0.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()


def set_tab_HW_end():
    """
提取心衰病人的身高/体重
    """
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_HW_end;"
                "create table tab_HW_end as "
                "with ch1 as "
                "(select hadm_id,"
                "case when(itemid='226512' or itemid='762' or itemid='763') "
                "and valuenum>0 then valuenum else null end as weight,"
                "case when(itemid='226707' or itemid='920' or itemid='1394' or itemid='3486') "
                "and valuenum>0 then valuenum*2.54 "
                "when(itemid='226730' or itemid='4188') "
                "and valuenum>0 then valuenum else null end as height "
                "from public.tab_hw_0)"
                "select hadm_id,"
                "avg(weight) as weight,"
                "avg(height) as height "
                "from ch1 "
                "group by hadm_id;")
    conn.commit()
    cur.close()
    conn.close()


def set_tab_RHNT_0():
    """
提取心衰病人每天检测的呼吸频率/心率/收缩压/舒张压/体温/氧饱和度（lab中也提取了）
    """
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_RHNT_0;"
                "create table tab_RHNT_0 as "
                "with ch2 as "
                "(select hadm_id,itemid,charttime,valuenum from mimiciii.chartevents "
                "where hadm_id in (select hadm_id from tab_hf_id) "
                "and (itemid='220210' or itemid='224690' or itemid='615' or itemid='618' "
                "or itemid='220045' or itemid='211' "
                "or itemid='6' or itemid='51' or itemid='442' or itemid='455' or itemid='6701' or itemid='220179' or itemid='220050' or itemid='225309' "
                "or itemid='8364' or itemid='8368' or itemid='8440' or itemid='8555' or itemid='220180' or itemid='220051' or itemid='225310' "
                "or itemid='223762' or itemid='676' or itemid='223761' or itemid='678' "
                "or itemid='220277') "
                "order by hadm_id)"
                "select ch2.hadm_id,ch2.itemid,ch2.valuenum,"
                "cast(date(ch2.charttime)-date(admissions.admittime) as integer) as now_day "
                "from ch2 join mimiciii.admissions "
                "on ch2.hadm_id = admissions.hadm_id "
                "order by ch2.hadm_id,cast(date(ch2.charttime)-date(admissions.admittime) as integer);")
    conn.commit()
    cur.close()
    conn.close()


def set_tab_RHNT_end():
    """
提取心衰病人每天检测的呼吸频率/心率/收缩压/舒张压/体温/氧饱和度（lab中也提取了）
    """
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_RHNT_end;"
                "create table tab_RHNT_end as "
                "with ch3 as "
                "(select hadm_id,now_day,"
                "case when(itemid='220210' or itemid='224690' or itemid='615' or itemid='618') "
                "then valuenum else null end as RR,"
                "case when(itemid='220045' or itemid='211') "
                "then valuenum else null end as HR,"
                "case when(itemid='6' or itemid='51' or itemid='442' or itemid='455' or itemid='6701' or itemid='220179' or itemid='220050' or itemid='225309') "
                "then valuenum else null end as NBP_HIGH,"
                "case when(itemid='8364' or itemid='8368' or itemid='8440' or itemid='8555' or itemid='220180' or itemid='220051' or itemid='225310') "
                "then valuenum else null end as NBP_LOW,"
                "case when(itemid='223762' or itemid='676') "
                "then valuenum when(itemid='223761' or itemid='678') "
                "then (valuenum-32)/9*5 else null end as temperature,"
                "case when(itemid='220277') "
                "then valuenum else null end as O2_SAT "
                "from tab_rhnt_0)"
                "select hadm_id,now_day,"
                "avg(RR) as RR,"
                "avg(HR) as HR,"
                "avg(NBP_HIGH) as NBP_HIGH,"
                "avg(NBP_LOW) as NBP_LOW,"
                "avg(temperature) as temperature,"
                "avg(O2_SAT) as O2_SAT "
                "from ch3 "
                "group by hadm_id,now_day;")
    conn.commit()
    cur.close()
    conn.close()


def set_tab_lab_0():
    """
提取心衰病人的生化检查数据
    """
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_lab_0;"
                "create table tab_lab_0 as "
                "with ch4 as "
                "(select labevents.hadm_id,labevents.itemid,labevents.charttime,labevents.valuenum "
                "from public.tab_hf_id join mimiciii.labevents "
                "on tab_hf_id.hadm_id = labevents.hadm_id "
                "where(itemid='50817' or itemid='50804' or itemid='50970' "
                "or itemid='50902' or itemid='50931' or itemid='50809' or itemid='50893' "
                "or itemid='50983' or itemid='50971' or itemid='50960' or itemid='51006' "
                "or itemid='50912' or itemid='50920' or itemid='51007' or itemid='50963' "
                "or itemid='51222' or itemid='50811' or itemid='50911' "
                "or itemid='51003' or itemid='50915' or itemid='51196' or itemid='51221' "
                "or itemid='50810' or itemid='50927' or itemid='50861' or itemid='50954' "
                "or itemid='50878' or itemid='50976' or itemid='50885' or itemid='50862' "
                "or itemid='50883' or itemid='50863'))"
                "select ch4.hadm_id,ch4.itemid,ch4.valuenum,"
                "cast(date(ch4.charttime)-date(admissions.admittime) as integer) as now_day "
                "from ch4 join mimiciii.admissions "
                "on ch4.hadm_id = admissions.hadm_id "
                "order by ch4.hadm_id,cast(date(ch4.charttime)-date(admissions.admittime) as integer);")
    conn.commit()
    cur.close()
    conn.close()


def set_tab_lab_end():
    """
提取心衰病人的生化检查数据
    """
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_lab_end;"
                "create table tab_lab_end as "
                "with ch5 as "
                "(select hadm_id,now_day,"
                "case when(itemid='50817') then valuenum else null end as oxygen_saturation,"
                "case when(itemid='50804') then valuenum else null end as co2,"
                "case when(itemid='50970') then valuenum else null end as phosphate,"
                "case when(itemid='50902') then valuenum else null end as chloride,"
                "case when(itemid='50931' or itemid='50809') then valuenum else null end as glucose,"
                "case when(itemid='50893') then valuenum else null end as calcium,"
                "case when(itemid='50983') then valuenum else null end as sodium,"
                "case when(itemid='50971') then valuenum else null end as potassium,"
                "case when(itemid='50960') then valuenum else null end as magnesium,"
                "case when(itemid='51006') then valuenum else null end as urea_nitrogen,"
                "case when(itemid='50912') then valuenum else null end as creatinine,"
                "case when(itemid='50920') then valuenum else null end as eGFR,"
                "case when(itemid='51007') then valuenum else null end as uric_acid,"
                "case when(itemid='50963') then valuenum else null end as ntprobnp,"
                "case when(itemid='51222' or itemid='50811') then valuenum else null end as hemoglobin,"
                "case when(itemid='50911') then valuenum else null end as ck_isoenzyme,"
                "case when(itemid='51003') then valuenum else null end as troponint,"
                "case when(itemid='50915' or itemid='51196') then valuenum else null end as d_dimer,"
                "case when(itemid='51221' or itemid='50810') then valuenum else null end as hematocrit,"
                "case when(itemid='50927') then valuenum else null end as gamma_glutamyltransferase,"
                "case when(itemid='50861') then valuenum else null end as alanine_aminotransferase,"
                "case when(itemid='50954') then valuenum else null end as lactate_dehydrogenase,"
                "case when(itemid='50878') then valuenum else null end as ast,"
                "case when(itemid='50976') then valuenum else null end as protein_total,"
                "case when(itemid='50885') then valuenum else null end as bilirubin_total,"
                "case when(itemid='50862') then valuenum else null end as albumin,"
                "case when(itemid='50883') then valuenum else null end as bilirubin_direct,"
                "case when(itemid='50863') then valuenum else null end as alkaline_phosphatase "
                "from tab_lab_0)"
                "select hadm_id,now_day,"
                "avg(oxygen_saturation) as oxygen_saturation,"
                "avg(co2) as co2,"
                "avg(phosphate) as phosphate,"
                "avg(chloride) as chloride,"
                "avg(glucose) as glucose,"
                "avg(calcium) as calcium,"
                "avg(sodium) as sodium,"
                "avg(potassium) as potassium,"
                "avg(magnesium) as magnesium,"
                "avg(urea_nitrogen) as urea_nitrogen,"
                "avg(creatinine) as creatinine,"
                "avg(eGFR) as eGFR,"
                "case when(avg(creatinine)>0) then avg(urea_nitrogen)/avg(creatinine) else null end as urea_nitrogen_creatinine_ratio,"
                "case when(avg(phosphate)>0) then avg(calcium)/avg(phosphate) else null end as calcium_phosphate_ratio,"
                "avg(uric_acid) as uric_acid,"
                "avg(ntprobnp) as ntprobnp,"
                "avg(hemoglobin) as hemoglobin,"
                "avg(ck_isoenzyme) as ck_isoenzyme,"
                "avg(troponint) as troponint,"
                "avg(d_dimer) as d_dimer,"
                "avg(hematocrit) as hematocrit,"
                "avg(gamma_glutamyltransferase) as gamma_glutamyltransferase,"
                "avg(alanine_aminotransferase) as alanine_aminotransferase,"
                "avg(lactate_dehydrogenase) as lactate_dehydrogenase,"
                "avg(ast) as ast,"
                "avg(protein_total) as protein_total,"
                "avg(bilirubin_total) as bilirubin_total,"
                "avg(albumin) as albumin,"
                "avg(bilirubin_direct) as bilirubin_direct,"
                "avg(alkaline_phosphatase) as alkaline_phosphatase "
                "from ch5 "
                "group by hadm_id,now_day;")
    conn.commit()
    cur.close()
    conn.close()


def set_tab_complication_0():
    """
提取心衰病人的并发症
    """
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_complication_0;"
                "create table tab_complication_0 as "
                "select hadm_id,icd9_code "
                "from mimiciii.diagnoses_icd "
                "where hadm_id in (select hadm_id from tab_hf_id);")
    conn.commit()
    cur.close()
    conn.close()


def set_tab_complication_end():
    """
提取心衰病人的并发症
    """
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_complication_end;"
                "create table tab_complication_end as "
                "with ch6 as "
                "(select hadm_id,"
                "case when(icd9_code in (select icd9_code from mimiciii.d_icd_diagnoses "
                "where long_title ~*'cardiomyopathy' or long_title ~*'cardiomyopathies')) "
                "then 1 else 0 end as cardiomyopathy,"
                "case when(icd9_code in (select icd9_code from mimiciii.d_icd_diagnoses "
                "where long_title ~*'valve')) "
                "then 1 else 0 end as valve,"
                "case when(icd9_code in (select icd9_code from mimiciii.d_icd_diagnoses "
                "where long_title ~*'Atrial flutter' or long_title ~*'Atrial fibrillation')) "
                "then 1 else 0 end as atrial,"
                "case when(icd9_code in (select icd9_code from mimiciii.d_icd_diagnoses "
                "where long_title ~*'hyperlipidemia' or long_title ~*'hypercholesterolemia' or long_title ~*'hyperglyceridemia')) "
                "then 1 else 0 end as hyperlipidemia,"
                "case when(icd9_code in (select icd9_code from mimiciii.d_icd_diagnoses "
                "where long_title ~*'hypertension' and not long_title ~*'pregnancy' and not long_title ~*'childbirth' "
                "and not long_title ~*'puerperium' and not long_title ~*'without' and not long_title ~*'Portal hypertension' "
                "and not long_title ~*'venous hypertension' and not long_title ~*'Screening for hypertension' and not long_title ~*'Family history')) "
                "then 1 else 0 end as hypertension,"
                "case when(icd9_code in (select icd9_code from mimiciii.d_icd_diagnoses "
                "where long_title ~*'Diabetes' and not long_title ~*'diabetes insipidus' and not long_title ~*'pregnancy' "
                "and not long_title ~*'Screening for diabetes mellitus' and not long_title ~*'Neonatal diabetes mellitus' "
                "and not long_title ~*'Family history')) "
                "then 1 else 0 end as diabetes,"
                "case when(icd9_code in (select icd9_code from mimiciii.d_icd_diagnoses "
                "where long_title ~*'sleep apnea')) "
                "then 1 else 0 end as apnea,"
                "case when(icd9_code in (select icd9_code from mimiciii.d_icd_diagnoses "
                "where long_title ~*'anemia' and not long_title ~*'Anemia of mother' or long_title ~*'Anemia of prematurity' "
                "and not long_title ~*'Screening' and not long_title ~*'Family history')) "
                "then 1 else 0 end as anemia,"
                "case when(icd9_code in (select icd9_code from mimiciii.d_icd_diagnoses "
                "where long_title ~*'infection')) "
                "then 1 else 0 end as infection,"
                "case when(icd9_code in (select icd9_code from mimiciii.d_icd_diagnoses "
                "where ((icd9_code>='410') and (icd9_code<='41407')) "
                "or ((icd9_code>='4142' and (icd9_code<='4149')) "
                "or icd9_code='42979'))) "
                "then 1 else 0 end as ischamic,"
                "case when(icd9_code in (select icd9_code from mimiciii.d_icd_diagnoses "
                "where long_title ~*'septicemia')) "
                "then 1 else 0 end as septicemia "
                "from tab_complication_0)"
                "select hadm_id,"
                "max(cardiomyopathy) as cardiomyopathy,"
                "max(valve) as valve,"
                "max(atrial) as atrial,"
                "max(hyperlipidemia) as hyperlipidemia,"
                "max(hypertension) as hypertension,"
                "max(diabetes) as diabetes,"
                "max(apnea) as apnea,"
                "max(anemia) as anemia,"
                "max(infection) as infection,"
                "max(ischamic) as ischamic,"
                "max(septicemia) as septicemia "
                "from ch6 "
                "group by hadm_id;")
    conn.commit()
    cur.close()
    conn.close()


def set_tab_drug_0():
    """
提取心衰病人药物使用情况信息
    """
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_drug_0;"
                "create table tab_drug_0 as "
                "with ch7 as "
                "(select hadm_id,date(startdate) as startdate,date(enddate) as enddate,drug "
                "from mimiciii.prescriptions "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "order by startdate),"
                "ch8 as "
                "(select ch7.hadm_id,ch7.drug,"
                "cast(date(startdate)-date(admittime) as integer) as start_day,"
                "cast(date(enddate)-date(admittime) as integer) as end_day "
                "from ch7 left join mimiciii.admissions "
                "on ch7.hadm_id = admissions.hadm_id),"
                "ch10 as "
                "(select hadm_id,"
                "case when drug ~*'Furosemide|Torsemide|Bumetanide' "
                "then 1 else 0 end as loop_diuretic,"
                "case when drug ~*'amiodarone' "
                "then 1 else 0 end as amiodarone,"
                "case when drug ~*'tolvaptan' "
                "then 1 else 0 end as tolvaptan,"
                "case when drug ~*'thiazide' "
                "then 1 else 0 end as hydrochlorothiazide,"
                "case when drug ~*'Triamterene|Spironolactone|Amiloride|Eplerenone' "
                "then 1 else 0 end as potassium_preserving,"
                "case when drug ~*'pril' and not drug ~* 'Lidocaine' "
                "then 1 else 0 end as acei,"
                "case when drug ~*'sartan|diovan' "
                "then 1 else 0 end as arb,"
                "case when drug ~*'lol' and not drug ~*'%|Flolan|timolol'"
                "then 1 else 0 end as brb,"
                "case when drug ~*'spironolactone|eplerenone' "
                "then 1 else 0 end as ald_receptor_antagonist,"
                "case when drug ~*'epinephrine|digoxin|dobutamine|milrinone|dopamine' "
                "and not drug ~*'phenylephrine|norepinephrine|%|Inhalation' "
                "then 1 else 0 end as cardiotonic,"
                "case when drug ~*'isordil|isosor|Nitro|Nitrate|Phentolamine' "
                "and not drug ~*'%|furantoin|Miconazole|silver' "
                "then 1 else 0 end as vasodilator,"
                "case when drug ~*'cef|penem|xacin|cillin|mycin|micin|conazole|Linezolid' "
                "and not drug ~*'%|Ophth'"
                "then 1 else 0 end as antibiotic "
                "from ch8) "
                "select hadm_id,"
                "max(loop_diuretic) AS loop_diuretic,"
                "max(amiodarone) AS amiodarone,"
                "max(tolvaptan) AS tolvaptan,"
                "max(hydrochlorothiazide) AS hydrochlorothiazide,"
                "max(potassium_preserving) AS potassium_preserving,"
                "max(acei) AS acei,"
                "max(arb) AS arb,"
                "max(brb) AS brb,"
                "max(ald_receptor_antagonist) AS ald_receptor_antagonist,"
                "max(cardiotonic) AS cardiotonic,"
                "max(vasodilator) AS vasodilator,"
                "max(antibiotic) AS antibiotic "
                "from ch10 "
                "group by hadm_id;")
    conn.commit()
    cur.close()
    conn.close()


def set_tab_drug_1():
    """
提取心衰病人药物使用情况信息
    """
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_drug_1;")
    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    set_tab_drug_0()
