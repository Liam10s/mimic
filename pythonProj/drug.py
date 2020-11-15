import numpy as np
import psycopg2


def set_tab_hf_drug():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_hf_drug;"
                "create table tab_hf_drug as "
                "select hadm_id,startdate,enddate,drug,"
                "dose_val_rx,dose_unit_rx,route "
                "from mimiciii.prescriptions "
                "where hadm_id in (select hadm_id from public.tab_hf_id);")
    conn.commit()
    cur.close()
    conn.close()

# prs=prescriptions
def set_tab_loopdiuretic_prs():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_loopdiuretic_prs;"
                "create table tab_loopdiuretic_prs as "
                "with "
                "ch1 as "
                "(select hadm_id,startdate,enddate,drug,"
                "dose_val_rx,dose_unit_rx,route "
                "from public.tab_hf_drug "
                "where drug ~* 'furosemide|torsemide|bumetanide')"
                "select ch1.hadm_id,ch1.drug,ch1.dose_val_rx,ch1.dose_unit_rx,ch1.route,"
                "cast(date(ch1.startdate)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.enddate)-date(admissions.admittime) as integer) as endday "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_amiodarone_prs():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_amiodarone_prs;"
                "create table tab_amiodarone_prs as "
                "with "
                "ch1 as "
                "(select hadm_id,startdate,enddate,drug,"
                "dose_val_rx,dose_unit_rx,route "
                "from public.tab_hf_drug "
                "where drug ~* 'amiodarone')"
                "select ch1.hadm_id,ch1.drug,ch1.dose_val_rx,ch1.dose_unit_rx,ch1.route,"
                "cast(date(ch1.startdate)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.enddate)-date(admissions.admittime) as integer) as endday "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_tolvaptan_prs():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_tolvaptan_prs;"
                "create table tab_tolvaptan_prs as "
                "with "
                "ch1 as "
                "(select hadm_id,startdate,enddate,drug,"
                "dose_val_rx,dose_unit_rx,route "
                "from public.tab_hf_drug "
                "where drug ~* 'tolvaptan')"
                "select ch1.hadm_id,ch1.drug,ch1.dose_val_rx,ch1.dose_unit_rx,ch1.route,"
                "cast(date(ch1.startdate)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.enddate)-date(admissions.admittime) as integer) as endday "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_hydrochlorothiazide_prs():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_hydrochlorothiazide_prs;"
                "create table tab_hydrochlorothiazide_prs as "
                "with "
                "ch1 as "
                "(select hadm_id,startdate,enddate,drug,"
                "dose_val_rx,dose_unit_rx,route "
                "from public.tab_hf_drug "
                "where drug ~* 'hydrochlorothiazide')"
                "select ch1.hadm_id,ch1.drug,ch1.dose_val_rx,ch1.dose_unit_rx,ch1.route,"
                "cast(date(ch1.startdate)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.enddate)-date(admissions.admittime) as integer) as endday "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_potassiumpreserving_prs():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_potassiumpreserving_prs;"
                "create table tab_potassiumpreserving_prs as "
                "with "
                "ch1 as "
                "(select hadm_id,startdate,enddate,drug,"
                "dose_val_rx,dose_unit_rx,route "
                "from public.tab_hf_drug "
                "where drug ~* 'triamterene|spironolactone|amiloride|eplerenone')"
                "select ch1.hadm_id,ch1.drug,ch1.dose_val_rx,ch1.dose_unit_rx,ch1.route,"
                "cast(date(ch1.startdate)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.enddate)-date(admissions.admittime) as integer) as endday "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_acei_prs():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_acei_prs;"
                "create table tab_acei_prs as "
                "with "
                "ch1 as "
                "(select hadm_id,startdate,enddate,drug,"
                "dose_val_rx,dose_unit_rx,route "
                "from public.tab_hf_drug "
                "where drug ~* 'pril' and not drug ~* 'lidocaine')"
                "select ch1.hadm_id,ch1.drug,ch1.dose_val_rx,ch1.dose_unit_rx,ch1.route,"
                "cast(date(ch1.startdate)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.enddate)-date(admissions.admittime) as integer) as endday "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_arb_prs():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_arb_prs;"
                "create table tab_arb_prs as "
                "with "
                "ch1 as "
                "(select hadm_id,startdate,enddate,drug,"
                "dose_val_rx,dose_unit_rx,route "
                "from public.tab_hf_drug "
                "where drug ~* 'sartan|diovan')"
                "select ch1.hadm_id,ch1.drug,ch1.dose_val_rx,ch1.dose_unit_rx,ch1.route,"
                "cast(date(ch1.startdate)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.enddate)-date(admissions.admittime) as integer) as endday "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_brb_prs():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_brb_prs;"
                "create table tab_brb_prs as "
                "with "
                "ch1 as "
                "(select hadm_id,startdate,enddate,drug,"
                "dose_val_rx,dose_unit_rx,route "
                "from public.tab_hf_drug "
                "where drug ~* 'lol' and not drug ~*'%|flolan|timolol')"
                "select ch1.hadm_id,ch1.drug,ch1.dose_val_rx,ch1.dose_unit_rx,ch1.route,"
                "cast(date(ch1.startdate)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.enddate)-date(admissions.admittime) as integer) as endday "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_aldreceptorantagonist_prs():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_aldreceptorantagonist_prs;"
                "create table tab_aldreceptorantagonist_prs as "
                "with "
                "ch1 as "
                "(select hadm_id,startdate,enddate,drug,"
                "dose_val_rx,dose_unit_rx,route "
                "from public.tab_hf_drug "
                "where drug ~* 'spironolactone|eplerenone')"
                "select ch1.hadm_id,ch1.drug,ch1.dose_val_rx,ch1.dose_unit_rx,ch1.route,"
                "cast(date(ch1.startdate)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.enddate)-date(admissions.admittime) as integer) as endday "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_cardiotonic_prs():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_cardiotonic_prs;"
                "create table tab_cardiotonic_prs as "
                "with "
                "ch1 as "
                "(select hadm_id,startdate,enddate,drug,"
                "dose_val_rx,dose_unit_rx,route "
                "from public.tab_hf_drug "
                "where drug ~* 'epinephrine|digoxin|dobutamine|milrinone|dopamine'"
                "and not drug ~* 'phenylephrine|norepinephrine|%|Inhalation')"
                "select ch1.hadm_id,ch1.drug,ch1.dose_val_rx,ch1.dose_unit_rx,ch1.route,"
                "cast(date(ch1.startdate)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.enddate)-date(admissions.admittime) as integer) as endday "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_vasodilator_prs():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_vasodilator_prs;"
                "create table tab_vasodilator_prs as "
                "with "
                "ch1 as "
                "(select hadm_id,startdate,enddate,drug,"
                "dose_val_rx,dose_unit_rx,route "
                "from public.tab_hf_drug "
                "where drug ~* 'isordil|isosor|Nitro|Nitrate|Phentolamine'"
                "and not drug ~* '%|furantoin|Miconazole|silver')"
                "select ch1.hadm_id,ch1.drug,ch1.dose_val_rx,ch1.dose_unit_rx,ch1.route,"
                "cast(date(ch1.startdate)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.enddate)-date(admissions.admittime) as integer) as endday "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_antibiotic_prs():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_antibiotic_prs;"
                "create table tab_antibiotic_prs as "
                "with "
                "ch1 as "
                "(select hadm_id,startdate,enddate,drug,"
                "dose_val_rx,dose_unit_rx,route "
                "from public.tab_hf_drug "
                "where drug ~* 'cef|penem|xacin|cillin|mycin|micin|conazole|Linezolid'"
                "and not drug ~* '%|Ophth')"
                "select ch1.hadm_id,ch1.drug,ch1.dose_val_rx,ch1.dose_unit_rx,ch1.route,"
                "cast(date(ch1.startdate)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.enddate)-date(admissions.admittime) as integer) as endday "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

# drug itemid
def find_loopdiuretic_id():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_loopdiuretic_id;"
                "create table tab_loopdiuretic_id as "
                "select label,itemid "
                "from mimiciii.d_items "
                "where label ~* 'Furosemide|Torsemide|Bumetanide';")
    conn.commit()
    cur.close()
    conn.close()

def find_amiodarone_id():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_amiodarone_id;"
                "create table tab_amiodarone_id as "
                "select label,itemid "
                "from mimiciii.d_items "
                "where label ~* 'amiodarone';")
    conn.commit()
    cur.close()
    conn.close()

def find_tolvaptan_id():    # null
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_tolvaptan_id;"
                "create table tab_tolvaptan_id as "
                "select label,itemid "
                "from mimiciii.d_items "
                "where label ~* 'tolvaptan';")
    conn.commit()
    cur.close()
    conn.close()

def find_hydrochlorothiazide_id():    # null
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_hydrochlorothiazide_id;"
                "create table tab_hydrochlorothiazide_id as "
                "select label,itemid "
                "from mimiciii.d_items "
                "where label ~* 'thiazide';")
    conn.commit()
    cur.close()
    conn.close()

def find_potassiumpreserving_id():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_potassiumpreserving_id;"
                "create table tab_potassiumpreserving_id as "
                "select label,itemid "
                "from mimiciii.d_items "
                "where label ~* 'Triamterene|Spironolactone|Amiloride|Eplerenone';")
    conn.commit()
    cur.close()
    conn.close()

def find_acei_id():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_acei_id;"
                "create table tab_acei_id as "
                "select label,itemid "
                "from mimiciii.d_items "
                "where label ~* 'pril' and not label ~* 'Lidocaine';")
    conn.commit()
    cur.close()
    conn.close()

def find_arb_id():  # null
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_arb_id;"
                "create table tab_arb_id as "
                "select label,itemid "
                "from mimiciii.d_items "
                "where label ~* 'sartan|diovan';")
    conn.commit()
    cur.close()
    conn.close()

def find_brb_id():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_brb_id;"
                "create table tab_brb_id as "
                "select label,itemid "
                "from mimiciii.d_items "
                "where label ~* 'lol' and not label ~* '%|Flolan|timolol';")
    conn.commit()
    cur.close()
    conn.close()

def find_aldreceptorantagonist_id():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_aldreceptorantagonist_id;"
                "create table tab_aldreceptorantagonist_id as "
                "select label,itemid "
                "from mimiciii.d_items "
                "where label ~* 'spironolactone|eplerenone';")
    conn.commit()
    cur.close()
    conn.close()

def find_cardiotonic_id():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_cardiotonic_id;"
                "create table tab_cardiotonic_id as "
                "select label,itemid "
                "from mimiciii.d_items "
                "where label ~* 'epinephrine|digoxin|dobutamine|milrinone|dopamine' "
                "and not label ~* 'phenylephrine|norepinephrine|%|Inhalation';")
    conn.commit()
    cur.close()
    conn.close()

def find_vasodilator_id():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_vasodilator_id;"
                "create table tab_vasodilator_id as "
                "select label,itemid "
                "from mimiciii.d_items "
                "where label ~* 'isordil|isosor|Nitro|Nitrate|Phentolamine' "
                "and not label ~* '%|furantoin|Miconazole|silver';")
    conn.commit()
    cur.close()
    conn.close()

def find_antibiotic_id():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_antibiotic_id;"
                "create table tab_antibiotic_id as "
                "select label,itemid "
                "from mimiciii.d_items "
                "where label ~* 'cef|penem|xacin|cillin|mycin|micin|conazole|Linezolid' "
                "and not label ~* '%|Ophth';")
    conn.commit()
    cur.close()
    conn.close()

# cv=inputevents_cv
def set_tab_loopdiuretic_cv():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_loopdiuretic_cv;"
                "create table tab_loopdiuretic_cv as "
                "with "
                "ch1 as "
                "(select hadm_id,charttime,itemid,amount,amountuom "
                "from mimiciii.inputevents_cv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and (itemid='3439' or itemid='45275' or itemid='46690' or itemid='228340' or itemid='221794'))"
                "select ch1.hadm_id,ch1.amount,ch1.amountuom,"
                "cast(date(ch1.charttime)-date(admissions.admittime) as integer) as inputday,"
                "case when (itemid='3439' or itemid='228340' or itemid='221794') then 'Furosemide/Lasix' "
                "when (itemid='45275' or itemid='46690') then 'Bumetanide' end as loop_diuretic "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_amiodarone_cv():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_amiodarone_cv;"
                "create table tab_amiodarone_cv as "
                "with "
                "ch1 as "
                "(select hadm_id,charttime,itemid,amount,amountuom "
                "from mimiciii.inputevents_cv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and (itemid='2478' or itemid='7158' or itemid='42342' "
                "or itemid='30112' or itemid='221347' or itemid='228339'))"
                "select ch1.hadm_id,ch1.amount,ch1.amountuom,"
                "cast(date(ch1.charttime)-date(admissions.admittime) as integer) as inputday,"
                "case when (itemid='2478' or itemid='7158' or itemid='42342' "
                "or itemid='30112' or itemid='221347' or itemid='228339') "
                "then 'amiodarone' end as amiodarone "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_potassiumpreserving_cv():   # null
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_potassiumpreserving_cv;"
                "create table tab_potassiumpreserving_cv as "
                "with "
                "ch1 as "
                "(select hadm_id,charttime,itemid,amount,amountuom "
                "from mimiciii.inputevents_cv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and (itemid='5962' or itemid='6296'))"
                "select ch1.hadm_id,ch1.amount,ch1.amountuom,"
                "cast(date(ch1.charttime)-date(admissions.admittime) as integer) as inputday,"
                "case when (itemid='5962' or itemid='6296') "
                "then 'spironolactone' end as potassium_preserving "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_acei_cv():  # null
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_acei_cv;"
                "create table tab_acei_cv as "
                "with "
                "ch1 as "
                "(select hadm_id,charttime,itemid,amount,amountuom "
                "from mimiciii.inputevents_cv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and (itemid='4810' or itemid='4823' or itemid='3349' or itemid='227694'))"
                "select ch1.hadm_id,ch1.amount,ch1.amountuom,"
                "cast(date(ch1.charttime)-date(admissions.admittime) as integer) as inputday,"
                "case when (itemid='4810' or itemid='4823' or itemid='227694') then 'Prilosec' "
                "when (itemid='3349') then 'Captopril' "
                "end as acei "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_brb_cv():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_brb_cv;"
                "create table tab_brb_cv as "
                "with "
                "ch1 as "
                "(select hadm_id,charttime,itemid,amount,amountuom "
                "from mimiciii.inputevents_cv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and (itemid='3174' or itemid='5282' or itemid='5292' or itemid='2953' "
                "or itemid='5162' or itemid='30122' or itemid='30117' or itemid='41458' "
                "or itemid='221429' or itemid='225974' or itemid='225153'))"
                "select ch1.hadm_id,ch1.amount,ch1.amountuom,"
                "cast(date(ch1.charttime)-date(admissions.admittime) as integer) as inputday,"
                "case when (itemid='3174' or itemid='30122' or itemid='225153') then 'Labetalol' "
                "when (itemid='5282' or itemid='5292' or itemid='5162' or itemid='41458') then 'propanolol' "
                "when (itemid='2953' or itemid='30117' or itemid='221429') then 'Esmolol' "
                "when (itemid='225974') then 'Metoprolol' "
                "end as brb "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_aldreceptorantagonist_cv():  # null
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_aldreceptorantagonist_cv;"
                "create table tab_aldreceptorantagonist_cv as "
                "with "
                "ch1 as "
                "(select hadm_id,charttime,itemid,amount,amountuom "
                "from mimiciii.inputevents_cv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and (itemid='5962' or itemid='6296'))"
                "select ch1.hadm_id,ch1.amount,ch1.amountuom,"
                "cast(date(ch1.charttime)-date(admissions.admittime) as integer) as inputday,"
                "case when (itemid='5962' or itemid='6296') then 'spironolactone' "
                "end as ald_receptor_antagonist "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_cardiotonic_cv():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_cardiotonic_cv;"
                "create table tab_cardiotonic_cv as "
                "with "
                "ch1 as "
                "(select hadm_id,charttime,itemid,amount,amountuom "
                "from mimiciii.inputevents_cv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and (itemid='4501' or itemid='801' or itemid='3112' or itemid='5747' or itemid='225644' "
                "or itemid='5805' or itemid='5329' or itemid='3751' or itemid='30119' "
                "or itemid='30125' or itemid='30043' or itemid='30306' or itemid='30307' "
                "or itemid='30309' or itemid='30042' or itemid='30044' or itemid='221289' "
                "or itemid='221653' or itemid='221662' or itemid='221986' or itemid='227440' ))"
                "select ch1.hadm_id,ch1.amount,ch1.amountuom,"
                "cast(date(ch1.charttime)-date(admissions.admittime) as integer) as inputday,"
                "case when (itemid='4501' or itemid='5805' or itemid='5329' or itemid='30043' "
                "or itemid='221662' or itemid='30307') then 'Dopamine' "
                "when (itemid='801' or itemid='3751' or itemid='227440' or itemid='225644') then 'Digoxin' "
                "when (itemid='3112' or itemid='30119' or itemid='30309' "
                "or itemid='30044' or itemid='221289') then 'Epinephrine' "
                "when (itemid='5747' or itemid='30306' or itemid='30042' or itemid='221653') then 'dobutamine' "
                "when (itemid='30125' or itemid='221986') then 'Milrinone' "
                "end as cardiotonic "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_vasodilator_cv():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_vasodilator_cv;"
                "create table tab_vasodilator_cv as "
                "with "
                "ch1 as "
                "(select hadm_id,charttime,itemid,amount,amountuom "
                "from mimiciii.inputevents_cv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and (itemid='2409' or itemid='30049' or itemid='30050' or itemid='30121' "
                "or itemid='43571' or itemid='222051' or itemid='222056'))"
                "select ch1.hadm_id,ch1.amount,ch1.amountuom,"
                "cast(date(ch1.charttime)-date(admissions.admittime) as integer) as inputday,"
                "case when (itemid='2409') then 'nitro' "
                "when (itemid='30049' or itemid='30121' or itemid='222056') then 'Nitroglycerine' "
                "when (itemid='30050' or itemid='222051') then 'Nitroprusside' "
                "when (itemid='43571') then 'nitrates' "
                "end as vasodilator "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_antibiotic_cv():    # null
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_antibiotic_cv;"
                "create table tab_antibiotic_cv as "
                "with "
                "ch1 as "
                "(select hadm_id,charttime,itemid,amount,amountuom "
                "from mimiciii.inputevents_cv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and itemid in (select  itemid from public.tab_antibiotic_id))"
                "select ch1.hadm_id,ch1.amount,ch1.itemid,ch1.amountuom,"
                "cast(date(ch1.charttime)-date(admissions.admittime) as integer) as inputday "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

# mv=inputevents_mv
def set_tab_loopdiuretic_mv():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_loopdiuretic_mv;"
                "create table tab_loopdiuretic_mv as "
                "with "
                "ch1 as "
                "(select hadm_id,starttime,endtime,itemid,amount,amountuom "
                "from mimiciii.inputevents_mv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and (itemid='3439' or itemid='45275' or itemid='46690' or itemid='228340' or itemid='221794'))"
                "select ch1.hadm_id,ch1.amount,ch1.amountuom,"
                "cast(date(ch1.starttime)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.endtime)-date(admissions.admittime) as integer) as endday,"
                "case when (itemid='3439' or itemid='228340' or itemid='221794') then 'Furosemide/Lasix' "
                "when (itemid='45275' or itemid='46690') then 'Bumetanide' end as loop_diuretic "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_amiodarone_mv():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_amiodarone_mv;"
                "create table tab_amiodarone_mv as "
                "with "
                "ch1 as "
                "(select hadm_id,starttime,endtime,itemid,amount,amountuom "
                "from mimiciii.inputevents_mv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and (itemid='2478' or itemid='7158' or itemid='42342' "
                "or itemid='30112' or itemid='221347' or itemid='228339'))"
                "select ch1.hadm_id,ch1.amount,ch1.amountuom,"
                "cast(date(ch1.starttime)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.endtime)-date(admissions.admittime) as integer) as endday,"
                "case when (itemid='2478' or itemid='7158' or itemid='42342' "
                "or itemid='30112' or itemid='221347' or itemid='228339') "
                "then 'amiodarone' end as amiodarone "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_potassiumpreserving_mv():   # null
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_potassiumpreserving_mv;"
                "create table tab_potassiumpreserving_mv as "
                "with "
                "ch1 as "
                "(select hadm_id,starttime,endtime,itemid,amount,amountuom "
                "from mimiciii.inputevents_mv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and (itemid='5962' or itemid='6296'))"
                "select ch1.hadm_id,ch1.amount,ch1.amountuom,"
                "cast(date(ch1.starttime)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.endtime)-date(admissions.admittime) as integer) as endday,"
                "case when (itemid='5962' or itemid='6296') "
                "then 'spironolactone' end as potassium_preserving "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_acei_mv():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_acei_mv;"
                "create table tab_acei_mv as "
                "with "
                "ch1 as "
                "(select hadm_id,starttime,endtime,itemid,amount,amountuom "
                "from mimiciii.inputevents_mv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and (itemid='4810' or itemid='4823' or itemid='3349' or itemid='227694'))"
                "select ch1.hadm_id,ch1.amount,ch1.amountuom,"
                "cast(date(ch1.starttime)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.endtime)-date(admissions.admittime) as integer) as endday,"
                "case when (itemid='4810' or itemid='4823' or itemid='227694') then 'Prilosec' "
                "when (itemid='3349') then 'Captopril' "
                "end as acei "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_brb_mv():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_brb_mv;"
                "create table tab_brb_mv as "
                "with "
                "ch1 as "
                "(select hadm_id,starttime,endtime,itemid,amount,amountuom "
                "from mimiciii.inputevents_mv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and (itemid='3174' or itemid='5282' or itemid='5292' or itemid='2953' "
                "or itemid='5162' or itemid='30122' or itemid='30117' or itemid='41458' "
                "or itemid='221429' or itemid='225974' or itemid='225153'))"
                "select ch1.hadm_id,ch1.amount,ch1.amountuom,"
                "cast(date(ch1.starttime)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.endtime)-date(admissions.admittime) as integer) as endday,"
                "case when (itemid='3174' or itemid='30122' or itemid='225153') then 'Labetalol' "
                "when (itemid='5282' or itemid='5292' or itemid='5162' or itemid='41458') then 'propanolol' "
                "when (itemid='2953' or itemid='30117' or itemid='221429') then 'Esmolol' "
                "when (itemid='225974') then 'Metoprolol' "
                "end as brb "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_aldreceptorantagonist_mv():  # null
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_aldreceptorantagonist_mv;"
                "create table tab_aldreceptorantagonist_mv as "
                "with "
                "ch1 as "
                "(select hadm_id,starttime,endtime,itemid,amount,amountuom "
                "from mimiciii.inputevents_mv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and (itemid='5962' or itemid='6296'))"
                "select ch1.hadm_id,ch1.amount,ch1.amountuom,"
                "cast(date(ch1.starttime)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.endtime)-date(admissions.admittime) as integer) as endday,"
                "case when (itemid='5962' or itemid='6296') then 'spironolactone' "
                "end as ald_receptor_antagonist "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_cardiotonic_mv():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_cardiotonic_mv;"
                "create table tab_cardiotonic_mv as "
                "with "
                "ch1 as "
                "(select hadm_id,starttime,endtime,itemid,amount,amountuom "
                "from mimiciii.inputevents_mv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and (itemid='4501' or itemid='801' or itemid='3112' or itemid='5747' or itemid='225644' "
                "or itemid='5805' or itemid='5329' or itemid='3751' or itemid='30119' "
                "or itemid='30125' or itemid='30043' or itemid='30306' or itemid='30307' "
                "or itemid='30309' or itemid='30042' or itemid='30044' or itemid='221289' "
                "or itemid='221653' or itemid='221662' or itemid='221986' or itemid='227440' ))"
                "select ch1.hadm_id,ch1.amount,ch1.amountuom,"
                "cast(date(ch1.starttime)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.endtime)-date(admissions.admittime) as integer) as endday,"
                "case when (itemid='4501' or itemid='5805' or itemid='5329' or itemid='30043' "
                "or itemid='221662' or itemid='30307') then 'Dopamine' "
                "when (itemid='801' or itemid='3751' or itemid='227440' or itemid='225644') then 'Digoxin' "
                "when (itemid='3112' or itemid='30119' or itemid='30309' "
                "or itemid='30044' or itemid='221289') then 'Epinephrine' "
                "when (itemid='5747' or itemid='30306' or itemid='30042' or itemid='221653') then 'dobutamine' "
                "when (itemid='30125' or itemid='221986') then 'Milrinone' "
                "end as cardiotonic "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_vasodilator_mv():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_vasodilator_mv;"
                "create table tab_vasodilator_mv as "
                "with "
                "ch1 as "
                "(select hadm_id,starttime,endtime,itemid,amount,amountuom "
                "from mimiciii.inputevents_mv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and (itemid='2409' or itemid='30049' or itemid='30050' or itemid='30121' "
                "or itemid='43571' or itemid='222051' or itemid='222056'))"
                "select ch1.hadm_id,ch1.amount,ch1.amountuom,"
                "cast(date(ch1.starttime)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.endtime)-date(admissions.admittime) as integer) as endday,"
                "case when (itemid='2409') then 'nitro' "
                "when (itemid='30049' or itemid='30121' or itemid='222056') then 'Nitroglycerine' "
                "when (itemid='30050' or itemid='222051') then 'Nitroprusside' "
                "when (itemid='43571') then 'nitrates' "
                "end as vasodilator "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

def set_tab_antibiotic_mv():
    conn = psycopg2.connect(database='mimicld', user='shiliangchen', password='inori7')
    cur = conn.cursor()
    cur.execute("drop table if exists tab_antibiotic_mv;"
                "create table tab_antibiotic_mv as "
                "with "
                "ch1 as "
                "(select hadm_id,starttime,endtime,itemid,amount,amountuom "
                "from mimiciii.inputevents_mv "
                "where hadm_id in (select hadm_id from public.tab_hf_id) "
                "and itemid in (select  itemid from public.tab_antibiotic_id))"
                "select ch1.hadm_id,ch1.amount,ch1.itemid,ch1.amountuom,"
                "cast(date(ch1.starttime)-date(admissions.admittime) as integer) as startday,"
                "cast(date(ch1.endtime)-date(admissions.admittime) as integer) as endday "
                "from ch1 left join mimiciii.admissions "
                "on ch1.hadm_id=admissions.hadm_id;")
    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    set_tab_antibiotic_mv()
