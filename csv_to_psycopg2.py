"""


author : @eduardo.
created : 21,July,2020
description : data_task_02 to perform data fetch operations on Open Street Map through overpy.Overpass() API 
requirements : psycop2 (pip install)


"""
import csv				#loading csv in python
import psycopg2           # to import the psqcopy library/driver

#set up connexion and table creation
conn = psycopg2.connect("host=localhost dbname=tfm_unigis user=postgres password='postgres_hhc'")
cur = conn.cursor()
cur.execute("""
    CREATE TABLE yeah_3 (
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
    id_2 text,
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
)
""")
with open('output_data.csv', encoding="utf8", mode='r') as f:
    reader = csv.reader(f)
    next(reader) # Skip the header row.
    for row in reader:
        cur.execute(
            "INSERT INTO yeah_3 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            row
        )
cur.execute ("""
    ALTER TABLE yeah_3 ADD COLUMN geom geometry(Point, 4326);
    UPDATE yeah_3 SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);
""")
conn.commit() # to commit chenges and create the table






