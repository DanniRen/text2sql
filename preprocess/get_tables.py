import os
import sys
import json
import pymysql
from os import listdir, makedirs
from os.path import isfile, isdir, join, split, exists, splitext
from nltk import word_tokenize, tokenize
import traceback

EXIST = {"atis", "geo", "advising", "yelp", "restaurants", "imdb", "academic"}

def convert_fk_index(data):
	fk_holder = []
	for fk in data["foreign_keys"]:
		tn, col, ref_tn, ref_col = fk[0][0], fk[0][1], fk[1][0], fk[1][1]
		ref_cid, cid = None, None
		try:
			tid = data['table_names_original'].index(tn)
			ref_tid = data['table_names_original'].index(ref_tn)

			for i, (tab_id, col_org) in enumerate(data['column_names_original']):
				if tab_id == ref_tid and ref_col == col_org:
					ref_cid = i
				elif tid == tab_id and col == col_org:
					cid = i
			if ref_cid and cid:
				fk_holder.append([cid, ref_cid])
		except:
			traceback.print_exc()
			print("table_names_original: ", data['table_names_original'])
			print("finding tab name: ", tn, ref_tn)
			sys.exit()
	return fk_holder


def dump_db_json_schema(config, db_id):
	'''read table and column info from StarRocks MySQL'''

	# Connect to StarRocks
	conn = pymysql.connect(
		host=config.get('host', 'localhost'),
		port=config.get('port', 9030),
		user=config.get('user', 'root'),
		password=config.get('password', ''),
		database=config.get('database', ''),
		charset='utf8mb4'
	)

	cursor = conn.cursor()

	# Get all table names
	cursor.execute("SHOW TABLES")
	tables = cursor.fetchall()

	data = {'db_id': db_id,
		 'table_names_original': [],
		 'table_names': [],
		 'column_names_original': [(-1, '*')],
		 'column_names': [(-1, '*')],
		 'column_types': ['text'],
		 'primary_keys': [],
		 'foreign_keys': []}

	fk_holder = []

	for i, item in enumerate(tables):
		table_name = item[0]
		data['table_names_original'].append(table_name)
		data['table_names'].append(table_name.lower().replace("_", ' '))

		# Get foreign key information from CREATE TABLE statement
		cursor.execute("SHOW CREATE TABLE `{}`".format(table_name))
		create_table_result = cursor.fetchone()
		if create_table_result and len(create_table_result) > 1:
			create_sql = create_table_result[1].lower()
			# Simple parsing for foreign keys - this is basic and might need enhancement
			if 'foreign key' in create_sql:
				# This is a simplified approach - you might need more sophisticated parsing
				pass  # StarRocks foreign key parsing would go here

		# Get column information
		cursor.execute("DESCRIBE `{}`".format(table_name))
		columns = cursor.fetchall()

		for j, col in enumerate(columns):
			col_name = col[0]
			col_type = col[1].lower()
			is_primary = col[3] == 'PRI'

			data['column_names_original'].append((i, col_name))
			data['column_names'].append((i, col_name.lower().replace("_", " ")))

			# Map MySQL types to the expected format
			if 'char' in col_type or 'text' in col_type or 'varchar' in col_type or 'string' in col_type:
				data['column_types'].append('text')
			elif 'int' in col_type or 'numeric' in col_type or 'decimal' in col_type or 'number' in col_type\
			 or 'id' in col_type or 'real' in col_type or 'double' in col_type or 'float' in col_type or 'bigint' in col_type:
				data['column_types'].append('number')
			elif 'date' in col_type or 'time' in col_type or 'year' in col_type or 'datetime' in col_type or 'timestamp' in col_type:
				data['column_types'].append('time')
			elif 'boolean' in col_type or 'bool' in col_type:
				data['column_types'].append('boolean')
			else:
				data['column_types'].append('others')

			if is_primary:
				data['primary_keys'].append(len(data['column_names'])-1)

	data["foreign_keys"] = fk_holder
	data['foreign_keys'] = convert_fk_index(data)

	cursor.close()
	conn.close()

	return data


if __name__ == '__main__':
	if len(sys.argv) < 3:
		print("Usage: python get_tables.py [config_file.json] [output_file.json] [db_id] [existing_tables.json]")
		print("Example: python get_tables.py starrocks_config.json output.json my_database existing.json")
		sys.exit()

	config_file = sys.argv[1]
	output_file = sys.argv[2]
	db_id = sys.argv[3]
	ex_tab_file = sys.argv[4] if len(sys.argv) > 4 else None

	# Load StarRocks configuration
	with open(config_file) as f:
		config = json.load(f)

	tables = []

	# Load existing tables if provided
	ex_tabs = {}
	if ex_tab_file and exists(ex_tab_file):
		with open(ex_tab_file) as f:
			ex_tabs_data = json.load(f)
			ex_tabs = {tab["db_id"]: tab for tab in ex_tabs_data}

	print('Reading StarRocks database: ', db_id)
	table = dump_db_json_schema(config, db_id)

	# If existing tables found, preserve table_names and column_names
	if db_id in ex_tabs:
		prev_tab_num = len(ex_tabs[db_id]["table_names"])
		prev_col_num = len(ex_tabs[db_id]["column_names"])
		cur_tab_num = len(table["table_names"])
		cur_col_num = len(table["column_names"])

		if prev_tab_num == cur_tab_num and prev_col_num == cur_col_num and prev_tab_num != 0 and len(ex_tabs[db_id]["column_names"]) > 1:
			table["table_names"] = ex_tabs[db_id]["table_names"]
			table["column_names"] = ex_tabs[db_id]["column_names"]

	tables.append(table)
	print("Final db num: ", len(tables))

	with open(output_file, 'w') as out:
		json.dump(tables, out, sort_keys=True, indent=2, separators=(',', ': '))