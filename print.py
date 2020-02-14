import psycopg2, io, os
import pandas as pd
from datetime import date, datetime

def connect():
	""" Connect to the PostgreSQL database server """
	conn = None
	try:
		print('Connecting to the PostgreSQL database...')
		conn = psycopg2.connect(host="", database="", port="", user="", password="")
		cur = conn.cursor()
		print('PostgreSQL database version:')
		cur.execute('SELECT version()')
		db_version = cur.fetchone()
		print(db_version)
		print('Running Query, please wait...')
		cur.execute("DROP TABLE IF EXISTS ISBNS; CREATE TEMP TABLE ISBNS AS SELECT regexp_replace(regexp_replace((v.field_content), '(\|[a-z])', '', 'i'), '(\|.*$)', '', 'i') as isbns, v.record_id, 'b' || v.record_num || 'a' as bibnum FROM sierra_view.varfield_view as v LEFT JOIN sierra_view.bib_record_location as bl ON bl.bib_record_id = v.record_id LEFT JOIN sierra_view.phrase_entry as z ON z.record_id = v.record_id WHERE v.marc_tag='020' AND bl.location_code not like '%es001%' AND bl.location_code not like '%j0001%' AND z.index_tag='i' GROUP BY isbns, v.record_id, v.record_num ORDER BY isbns; SELECT d.isbns, d.bibnum FROM ISBNS as d JOIN sierra_view.record_metadata as x ON d.record_id=x.id WHERE d.isbns IN (SELECT y.isbns FROM ISBNS as y GROUP BY y.isbns HAVING count(y.record_id)>1) AND x.record_last_updated_gmt >= ('{}') ORDER BY d.bibnum".format(date.today(), '%y-%m-%d'))
		rows=cur.fetchall()
		countrows = cur.rowcount
		mytable = []
		colnames=['isbn', 'bibnum']
		print("The number of duplicates: ", cur.rowcount)
		for row in rows:
			mytable.append(row)
		df = pd.DataFrame(data=mytable, columns=colnames)
		print(df)
		my_list = "PostgreSQL database version: {}\n\nThis Query will return isbns that appear in more than one bib that is neither electronic nor JCB.\n\nUpdated: {}\n\nThe number of duplicates: {}\n{}".format(db_version, datetime.now(), countrows, df)
		with io.open(os.path.join(os.path.expanduser('~'), "Desktop", "duplicate_isbns.txt"), 'w', encoding="utf-8") as f:
			f.write(my_list)
		f.close()
		cur.close()
	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
			print('Database connection closed.')
			

if __name__ == '__main__':
    connect()
