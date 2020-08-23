"""


author : @eduardo.
created : 20,July,2020
description : data_task_02 to perform data fetch operations on Open Street Map through overpy.Overpass() API 
requirements : overpy==0.4 (pip install overpy)
							 requests    (pip install requests)


"""

import overpy           # to import the overpy module
import pandas as pd     # to import pandas library
import json 						# to import json
import requests					# to import requests
import csv				#loading csv in python
import psycopg2           # to import the psqcopy library/driver



#this function gets the input from user.  INPUT = {laitutde, longitude, search_radius, option to specify the data domain like hospital,education etc.}
def get_input():
	print("\nEnter latitude (example->'40.416981') >> ")
	latitude = input()
	print("\nEnter longitude (example->'-3.703218') >> ")
	longitude = input()
	print("\nEnter scan radius for target.(in meters) (EXAMPLE->'20000') >> ")
	search_radius = input()
	print("\nEnter an option.(integer) :\n1. Amenities Data\n2. Shops Data\n3. Amenities&Shops Data")
	option = int(input("\n>>>"))
	while option not in [1,2,3]: 
		print("Invalid Option. Try Again \n>>")
		option = int(input())
	return([latitude,longitude,search_radius,option])   #returns the list of user inputs


#this function arrenge user inputs to build the 'query' (in overpass QL language) for amenity food related data and returns the query
def get_amenity_query(user_input):
	prefix = """[out:json][timeout:50];("""  				          	#this is string of syntex in 'Overpass QL' language
	fast_foodnode="""node["amenity"="fast_food"](around:""" 		  	  #this is string of syntex in 'Overpass QL' language
	marketplacenode="""node["amenity"="marketplace"](around:"""		  	  #this is string of syntex in 'Overpass QL' language
	suffix = """);out body;>;out skel qt;"""				        	  #this is string of syntex in 'Overpass QL' language
	q = user_input[2]+','+user_input[0]+','+user_input[1]    	  #(radius,latitude,longitude) in a string form the user input
	built_query = prefix + fast_foodnode+ q +');'+ marketplacenode+ q +');' + suffix  #combine all the above strings in correct order to form a query
	return built_query											                    #returns the complete overpass query


#this function arrenge user inputs to build the 'query' (in overpass QL language) for shop food related data and returns the query
def get_shop_query(user_input):
	prefix = """[out:json][timeout:50];("""  				          	#this is string of syntex in 'Overpass QL' language
	bakerynode="""node["shop"="bakery"](around:""" 		  	  #this is string of syntex in 'Overpass QL' language
	butchernode="""node["shop"="butcher"](around:"""		  	  #this is string of syntex in 'Overpass QL' language
	conveniencenode="""node["shop"="convenience"](around:""" 		  	  #this is string of syntex in 'Overpass QL' language
	dairynode="""node["shop"="dairy"](around:"""		  	  #this is string of syntex in 'Overpass QL' language
	frozen_foodnode="""node["shop"="frozen_food"](around:""" 		  	  #this is string of syntex in 'Overpass QL' language
	greengrocernode="""node["shop"="greengrocer"](around:"""		  	  #this is string of syntex in 'Overpass QL' language
	health_foodnode="""node["shop"="health_food"](around:""" 		  	  #this is string of syntex in 'Overpass QL' language
	pastrynode="""node["shop"="pastry"](around:"""		  	  #this is string of syntex in 'Overpass QL' language
	seafoodnode="""node["shop"="seafood"](around:""" 		  	  #this is string of syntex in 'Overpass QL' language
	supermarketnode="""node["shop"="supermarket"](around:"""		  	  #this is string of syntex in 'Overpass QL' language
	suffix = """);out body;>;out skel qt;"""				        	  #this is string of syntex in 'Overpass QL' language
	q = user_input[2]+','+user_input[0]+','+user_input[1]    	  #(radius,latitude,longitude) in a string form the user input
	built_query = prefix + bakerynode+ q +');'+ butchernode+ q +');' + conveniencenode+ q +');' + dairynode+ q +');' + frozen_foodnode+ q +');' + greengrocernode+ q +');' + health_foodnode+ q +');' + pastrynode+ q +');' + seafoodnode+ q +');' + supermarketnode+ q +');' + suffix  #combine all the above strings in correct order to form a query
	return built_query											                    #returns the complete overpass query



#this function arrenge user inputs to build the 'query' (in overpass QL language) for shop&amenities food related data and returns the query
def get_shop_and_amenities_query(user_input):
	prefix = """[out:json][timeout:50];("""  				          	#this is string of syntex in 'Overpass QL' language
	bakerynode="""node["shop"="bakery"](around:""" 		  	  #this is string of syntex in 'Overpass QL' language
	butchernode="""node["shop"="butcher"](around:"""		  	  #this is string of syntex in 'Overpass QL' language
	conveniencenode="""node["shop"="convenience"](around:""" 		  	  #this is string of syntex in 'Overpass QL' language
	dairynode="""node["shop"="dairy"](around:"""		  	  #this is string of syntex in 'Overpass QL' language
	frozen_foodnode="""node["shop"="frozen_food"](around:""" 		  	  #this is string of syntex in 'Overpass QL' language
	greengrocernode="""node["shop"="greengrocer"](around:"""		  	  #this is string of syntex in 'Overpass QL' language
	health_foodnode="""node["shop"="health_food"](around:""" 		  	  #this is string of syntex in 'Overpass QL' language
	pastrynode="""node["shop"="pastry"](around:"""		  	  #this is string of syntex in 'Overpass QL' language
	seafoodnode="""node["shop"="seafood"](around:""" 		  	  #this is string of syntex in 'Overpass QL' language
	supermarketnode="""node["shop"="supermarket"](around:"""		  	  #this is string of syntex in 'Overpass QL' language
	fast_foodnode="""node["amenity"="fast_food"](around:""" 		  	  #this is string of syntex in 'Overpass QL' language
	marketplacenode="""node["amenity"="marketplace"](around:"""		  	  #this is string of syntex in 'Overpass QL' language
	suffix = """);out body;>;out skel qt;"""				        	  #this is string of syntex in 'Overpass QL' language
	q = user_input[2]+','+user_input[0]+','+user_input[1]    	  #(radius,latitude,longitude) in a string form the user input
	built_query = prefix + bakerynode+ q +');'+ butchernode+ q +');' + conveniencenode+ q +');' + dairynode+ q +');' + frozen_foodnode+ q +');' + greengrocernode+ q +');' + health_foodnode+ q +');' + pastrynode+ q +');' + seafoodnode+ q +');' + supermarketnode+ q +');' + fast_foodnode+ q +');' + marketplacenode+ q +');' + suffix  #combine all the above strings in correct order to form a query
	return built_query



# this funciton uses the overpy.Overpass API to send a query and get the response from overpass servers in json format and then it extract the nodes(hospitals , schools) data to a csv file.
def extract_nodes_data_from_OSM(built_query):
	api = overpy.Overpass()                       # creating a overpass API instance 
	result = api.query(built_query)               # get result from API by sending the query to overpass servers
	list_of_node_tags = []                        # initializing empty list , we'll use it to form a dataframe .
	for node in result.nodes:                     # from each node , get the all tags information
		node.tags['latitude'] =  node.lat
		node.tags['longitude'] = node.lon
		node.tags['id'] = node.id
		list_of_node_tags.append(node.tags)
	data_frame = pd.DataFrame(list_of_node_tags)  # forming a pandas dataframe using list of dictionaries
	data_frame.to_csv('output_data.csv')
	print("\nCSV file created- 'output_data.csv'. Check the file in current directory.")
	return data_frame                             # return data frame if you want to use it further in main function.



# this function only extracts the raw  json data from overpass api through get request
def extract_raw_data_from_OSM(built_query):
	overpass_url = "http://overpass-api.de/api/interpreter" 					 #url of overpass api
	response = requests.get(overpass_url,params={'data': built_query}) # sending a get request and passing the overpass query as data parameter in url
	print(response.text)
	json_data = response.json()
	with open("output_data.json", "w") as outfile:  									 # writing the json output to a file
		json.dump(json_data, outfile)
	print("Raw Data extraction successfull!  check 'output_data.json' file.")
	return json_data
 
	
	
#this function import previous csv data into postgres. It also make a 4326 geom field:

def csv_data_into_postgres():
	#set up connexion and table creation
	conn = psycopg2.connect("host=localhost dbname=tfm_unigis user=postgres password='postgres_hhc'")
	cur = conn.cursor()
	cur.execute("""
		CREATE TABLE comercios_alimentacion_video (
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
		fuel_diesel text,
		fuel_octane_95 text,
		fuel_octane_98 text,
		payment_coins text,
		payment_maestro text,
		payment_mastercard text,
		payment_notes text,
		payment_visa text,
		payment_visa_debit text,
		payment_visa_electron text,
		fuel_GTL_diesel text,
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
		email text,
		addr_subdistrict text,
		access_covid19 text,
		drive_through_covid19 text,
		official_name text,
		old_name_2 text,
		name_ru text,
		payment_american_express text,
		payment_diners_club text,
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
		disused text,
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
		addr_unit text,
		addr_place text,
		payment_meal_voucher text,
		bakery text,
		payment_telephone_cards text
	)
	""")
	with open('output_data.csv', encoding="utf8", mode='r') as f:
		reader = csv.reader(f)
		next(reader) # Skip the header row.
		for row in reader:
			cur.execute(
				"INSERT INTO comercios_alimentacion_video VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
				row
			)
	cur.execute ("""
		ALTER TABLE comercios_alimentacion_video ADD COLUMN geom geometry(Point, 4326);
		UPDATE comercios_alimentacion_video SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);
	""")
	conn.commit() # to commit changes and create the table



if __name__ == '__main__':  #main function to act accordingly to the user's input.

	user_input=get_input()
	option = user_input[3]
	if(option==1):
		query = get_amenity_query(user_input)
		data_frame= extract_nodes_data_from_OSM(query)
	elif(option==2):
		query = get_shop_query(user_input)
		data_frame= extract_nodes_data_from_OSM(query)
	elif(option==3):
		query = get_shop_and_amenities_query(user_input)
		data_frame= extract_nodes_data_from_OSM(query)
		csv_data_into_postgres()
	print("Note: \n1. Please rename the output file, so that it can't be overwritten when you execute this program again.\n2. output file shouldn't remain open while running this program, because writing will perform on the output file while executing the program next time. ")



        






