# -*- coding: utf-8 -*-

def getKeywordId(DB,keyword):
	c = DB.cursor()
	query = "SELECT id FROM keywords WHERE keyword = %s"
	c.execute(query,(keyword))
	row = c.fetchone()

	if row:
		keyword_id = row[0]
	else:
		query = "INSERT INTO keywords (keyword) VALUES(%s)"
		c.execute(query,(keyword))
		keyword_id = c.lastrowid

	c.close()
	return keyword_id
