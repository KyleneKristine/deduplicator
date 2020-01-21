import psycopg2
from datetime import date, datetime

def connect():
	""" Connect to the PostgreSQL database server """
	conn = None
	try:
		print('Connecting to the PostgreSQL database...')
		conn = psycopg2.connect(host="brown-db.iii.com", database="iii", port="1032", user="INSERTUN", password="INSERTPW")
		cur = conn.cursor()
		print('PostgreSQL database version:')
		cur.execute('SELECT version()')
		db_version = cur.fetchone()
		print(db_version)
		print(datetime.now())
		print('Running Query, please wait...')
		cur.execute("DROP TABLE IF EXISTS DDAs; CREATE TEMP TABLE DDAs AS SELECT v.field_content as tags, v.record_id, v.record_num, z.index_entry as isbns, z.id, z.index_tag FROM sierra_view.varfield_view as v LEFT JOIN sierra_view.bib_record_location as bl ON bl.bib_record_id = v.record_id LEFT JOIN sierra_view.phrase_entry as z ON z.record_id = v.record_id WHERE z.record_id=v.record_id AND z.index_tag='i' AND v.marc_tag='910' AND bl.location_code like '%es001%' AND (v.field_content LIKE '|aJSTOR%' OR v.field_content LIKE '|aGOBI%' OR v.field_content LIKE '|aybp%'); SELECT d.tags, d.isbns, 'b' || d.record_num || 'a' as rec_num FROM DDAs as d JOIN sierra_view.record_metadata as x ON d.record_id=x.id WHERE d.isbns IN (SELECT d.isbns FROM DDAs as d WHERE d.index_tag='i' GROUP BY d.isbns HAVING count(d.id)>1) AND x.record_last_updated_gmt >= ('{}') GROUP BY rec_num, d.isbns, d.tags ORDER BY d.isbns".format(date.today(), '%y-%m-%d'))
		rows=cur.fetchall()
		print("The number of duplicates: ", cur.rowcount)
		for row in rows:
			print(row)
		print(datetime.now())
		cur.close()
	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
			print('Database connection closed.')

 
if __name__ == '__main__':
    connect()
