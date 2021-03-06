CREATE TABLE IF NOT EXISTS amenitiesandshops (
id integer PRIMARY KEY, 
amenity text,
brand text,
brand_wikidata text,
brand_wikipedia text,
cuisine text,
drive_through text,
name text,
latitude real,
longitude real,
id_2 integer,
shop text,
takeaway text,
note text,
addr_housenumber integer,
addr_postcode integer,
addr_street text,
internet_access text,
operator text,
source text,
addr_country text,
contact_website text,
created_by text,
addr_city text,
organic text,
wheelchair text,
name_en text,
name_es text,
wheelchair_description text,
old_name text,
alt_name text,
website text,
old_name_2013 text,
old_name_2013_2018 text,
opening_hours text,
phone integer
);


CREATE TABLE IF NOT EXISTS amenitiesandshops (
id integer PRIMARY KEY, 
amenity text,
brand text,
brand_wikidata text,
brand_wikipedia text,
cuisine text,
drive_through text,
name text,
latitude real,
longitude real,
id_2 integer,
shop text,
takeaway text,
note text,
addr_housenumber text,
addr_postcode text,
addr_street text,
internet_access text,
operator text,
source text,
addr_country text,
contact_website text,
created_by text,
addr_city text,
organic text,
wheelchair text,
name_en text,
name_es text,
wheelchair_description text,
old_name text,
alt_name text,
website text,
old_name_2013 text,
old_name_2013_2018 text,
opening_hours text,
phone text,
wikidata text,
wikipedia text,
educamadrid_codigo_postal text,
educamadrid_distrito text,
educamadrid_municipio text,
educamadrid_nombre_via text,
educamadrid_numero_portal text,
educamadrid_tipo_via text,
delivery text,
outdoor_seating text,
atm text,
fuel_GTL_diesel text,
fuel_diesel text,
fuel_octane_95 text,
fuel_octane_98 text,
contact_fax text,
contact_phone text,
payment_cash text,
payment_credit_cards text,
entrance text,
toilets_wheelchair text,
name_zh text,
delivery_covid19 text,
opening_hours_covid19 text,
takeaway_covid19 text,
layer text,
diet_gluten_free text,
diet_vegan text,
diet_vegetarian text,
reservation text,
foot text,
level text,
addr_housename text,
int_name text,
opening_hours_es text,
drink_club_mate text,
description text,
addr_province text,
addr_state text,
old_name_1 text,
operator_wikidata text,
operator_wikipedia text,
source_date text,
old_name1 text,
short_name text,
designation text,
disused_shop text,
addr text,
name_pt text,
toilets text,
toilets_access text,
noname text,
contact_facebook text,
craft text,
payment_coins text,
payment_notes text,
contact_email text,
disused_amenity text,
drive_in text,
precio_menudia text,
internet_access_fee text,
phone_ES text,
payment_bitcoin text,
fixme text,
smoking text,
nohousenumber text,
payment_contactless text,
payment_mastercard text,
payment_visa text,
email text,
addr_subdistrict text,
access_covid19 text,
drive_through_covid19 text,
official_name text,
old_name_2 text,
name_ru text,
payment_american_express text,
payment_diners_club text,
payment_maestro text,
payment_visa_debit text,
payment_visa_electron text,
butcher text,
old_name_1986 text,
old_name_1986_2016 text,
capacity text,
addr_ward text,
check_date text,
wheelchair_description_es text,
fair_trade text,
contact_twitter text,
changing_table text,
building text,
contact_cellphone text,
name_uk text,
addr_district text,
name_de text,
source_url text,
source_web text,
payment_debit_cards text,
building_levels text,
payment_bancomat text,
payment_discover_card text,
payment_electronic_purses text,
payment_ep_avant text,
payment_ep_chipknip text,
payment_ep_geldkarte text,
payment_ep_mep text,
payment_girocard text,
payment_jcb text,
payment_laser text,
payment_meal_voucher text,
bakery text,
payment_telephone_cards text,
payment_twyp text,
name_ja text,
beacon_type text,
indoormark text,
payment_cards text,
start_date text,
name_it text,
addr_unit text
);


cur.execute("""
CREATE TABLE amenitiesandshops (
	id text PRIMARY KEY, 
	amenity text,
	brand text,
	brand_wikidata text,
	brand_wikipedia text,
	cuisine text,
	drive_through text,
	name text,
	latitude text,
	longitude text
)

""")


with open('output_data_cleaned.csv', 'r') as f:
    next(f) # Skip the header row.
    cur.copy_from(f, 'amenitiesandshops', sep=',')


f = open(r'C:\Users\hugoh\Desktop\UNIGIS_documentacion\osm_dataextraction script\OpenStreetMapDataExtraction\output_data_cleaned.csv', 'r')
cur.copy_from(f, temp_unicommerce_status, sep=',')
f.close()




It workssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
import csv				#loading csv in python
import psycopg2           # to import the psqcopy library/driver

#set up connexion and table creation
conn = psycopg2.connect("host=localhost dbname=tfm_unigis user=postgres password='postgres_hhc'")
cur = conn.cursor()
with open('output_data_cleaned.csv', encoding="utf8", mode='r') as f:
    reader = csv.reader(f)
    next(reader) # Skip the header row.
    for row in reader:
        cur.execute(
            "INSERT INTO amenitiesandshops VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            row
        )
conn.commit() # to commit chenges and create the table


SELECT
	t1_1
FROM
	indicadores_madrid as indicadores
INNER JOIN secciones_madrid_deinteres as zonascensales
ON (indicadores.ccaa = zonascensales.cca and indicadores.cpro = zonascensales.cpro and indicadores.cmun = zonascensales.cmun and indicadores.dist = zonascensales.cdis and indicadores.secc = zonascensales.csec);



SELECT
	indicadores.t1_1,
	indicadores.cmun,
	indicadores.dist,
	indicadores.secc,
	zonascensales.geom,
	zonascensales.cmun,
	zonascensales.cdis,
	zonascensales.csec,
	zonascensales.nmun,
	zonascensales.shape_area
FROM
	indicadores_madrid as indicadores
INNER JOIN secciones_madrid_deinteres as zonascensales
ON (indicadores.ccaa = zonascensales.cca and indicadores.cpro = zonascensales.cpro and indicadores.cmun = zonascensales.cmun and indicadores.dist = zonascensales.cdis and indicadores.secc = zonascensales.csec);




CREATE TABLE indicadores_zonascensales_join AS
SELECT
	indicadores.t1_1 as t1_1_indic,
	indicadores.cmun as cmun_indic,
	indicadores.dist as dist_indic,
	indicadores.secc as secc_indic,
	zonascensales.geom,
	zonascensales.id as id_zonas,
	zonascensales.objectid as objectid_zonas,
	zonascensales.cmun as cmun_zonas,
	zonascensales.cdis as cdis_zonas,
	zonascensales.csec as csec_zonas,
	zonascensales.nmun as nmun_zonas,
	zonascensales.shape_area
FROM
	indicadores_madrid as indicadores
INNER JOIN secciones_madrid_deinteres as zonascensales
ON (indicadores.ccaa = zonascensales.cca and indicadores.cpro = zonascensales.cpro and indicadores.cmun = zonascensales.cmun and indicadores.dist = zonascensales.cdis and indicadores.secc = zonascensales.csec);


--Spatial JOIN / Intersect para counting
CREATE TABLE comercios_por_zonacensal as
SELECT z.objectid_zonas,  COUNT (*) as nodes_count
FROM indicadores_zonascensales_join as z
	JOIN comercios_alimentacion as c
	ON ST_INTERSECTS(z.geom, c.geom)
	GROUP BY z.objectid_zonas
	ORDER BY nodes_count DESC
--Resultado: de las 506 zonas censales escogidas, hay comercios registrados en osm en sólo 228 de ellas. 278 no tienen comercios alimentacion.


--Cambio el nombre del campo objectid_zonas a objid y ambos campos el datatype de bigint a integer:
ALTER TABLE comercios_por_zonacensal
RENAME COLUMN objectid_zonas TO objid_zonas;

ALTER TABLE comercios_por_zonacensal
ALTER COLUMN objid_zonas TYPE INT,
ALTER COLUMN nodes_count TYPE INT;

----Spatial JOIN / Intersect para geometrias:
CREATE TABLE comercios_por_zonacensal_geometrias as
SELECT c.id, c.geom, c.amenity, c.brand, c.cuisine, c.name, c.id_2, c.shop
FROM indicadores_zonascensales_join as z
	JOIN comercios_alimentacion as c
	ON ST_INTERSECTS(z.geom, c.geom)


--Ahora me tengo que llevar el counting de comercios a la tabla de indicadores_zonascensales_join mediante un outer join con el campo comun de ambas tablas "objectid":
CREATE TABLE indicadores_zonascenso_comercios as
SELECT * FROM indicadores_zonascensales_join as z
LEFT OUTER JOIN comercios_por_zonacensal c
ON z.objectid_zonas = c.objid_zonas;



--Creo un nuevo campo para el indice que se pide:
ALTER TABLE indicadores_zonascenso_comercios ADD tfm_indice numeric(5, 3);
--Ahora seria dividir num hab entre num comercios * 100,tal y como dice el pdf, o al reves, num comercios/habitantes *100:
UPDATE indicadores_zonascenso_comercios 
SET tfm_indice = (nodes_count/t1_1_indic::numeric)*1000;
--NOTA: multiplico por 1000 y no por 100 como he leido en el foro.

