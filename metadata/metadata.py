# -*- coding: utf-8 -*-

from common import common, database
import re
from lxml import etree
from StringIO import StringIO
import MySQLdb


class UpdateMetadata:
	def __init__(self,config,action,identifier,path):
		self.DB = MySQLdb.connect(host=config["db_host"],user=config["db_user"], passwd=config["db_passwd"],db=config["db_name"],use_unicode=1)

		self.identifier = identifier
		self.path = path
		self.title = ""
		self.description = ""
		self.aggregationlvl = 0
		self.location = ""
		self.keywords = []
		self.contexts = []
		self.disciplines = []
		self.educationallevels = []

		self.re_uuid = re.compile(common.getRe('uuid'), re.I)
		self.re_dash = re.compile(common.getRe('dash'))

		if action == "insert":
			self.extractData()
			self.storeData()
		elif action == "delete":
			self.deleteData()


	def extractData(self):
		f = open(self.path, 'r')
		xmlstr = f.read()
		f.close()
		xmltree = etree.parse(StringIO(xmlstr))
		
		ns={'lom':'http://www.imsglobal.org/xsd/imsmd_v1p2'}
		
		if xmltree.xpath('/lom:lom/lom:general/lom:title/lom:langstring', namespaces=ns):
			self.title = xmltree.xpath('/lom:lom/lom:general/lom:title/lom:langstring', namespaces=ns)[0].text

		if xmltree.xpath('/lom:lom/lom:general/lom:description/lom:langstring', namespaces=ns):
			self.description = xmltree.xpath('/lom:lom/lom:general/lom:description/lom:langstring', namespaces=ns)[0].text
		
		if xmltree.xpath('/lom:lom/lom:general/lom:aggregationlevel/lom:value/lom:langstring',namespaces=ns):
			try:
				self.aggregationlvl = int(xmltree.xpath('/lom:lom/lom:general/lom:aggregationlevel/lom:value/lom:langstring', namespaces=ns)[0].text)
			except:
				self.aggregationlvl = 0

		if xmltree.xpath('/lom:lom/lom:technical/lom:location', namespaces=ns):
			self.location = xmltree.xpath('/lom:lom/lom:technical/lom:location', namespaces=ns)[0].text

		if xmltree.xpath('/lom:lom/lom:general/lom:keyword/lom:langstring', namespaces=ns):
			for kw in xmltree.xpath('/lom:lom/lom:general/lom:keyword/lom:langstring', namespaces=ns):
				self.keywords.append(kw.text)

		if xmltree.xpath('/lom:lom/lom:educational/lom:context/lom:value/lom:langstring', namespaces=ns):
			for context in xmltree.xpath('/lom:lom/lom:educational/lom:context/lom:value/lom:langstring', namespaces=ns):
				self.contexts.append(context.text)

		if xmltree.xpath("/lom:lom/lom:classification/lom:purpose/lom:value/lom:langstring[text()='discipline']/../../../lom:taxonpath/lom:taxon/lom:id", namespaces=ns):
			for vak in xmltree.xpath("/lom:lom/lom:classification/lom:purpose/lom:value/lom:langstring[text()='discipline']/../../../lom:taxonpath/lom:taxon/lom:id", namespaces=ns):
				self.disciplines.append(vak.text)

		if xmltree.xpath("/lom:lom/lom:classification/lom:purpose/lom:value/lom:langstring[text()='educational level']/../../../lom:taxonpath/lom:taxon/lom:id", namespaces=ns):
			for lvl in xmltree.xpath("/lom:lom/lom:classification/lom:purpose/lom:value/lom:langstring[text()='educational level']/../../../lom:taxonpath/lom:taxon/lom:id", namespaces=ns):
				self.educationallevels.append(lvl.text)


	def storeData(self):
		c = self.DB.cursor()
		# first, find out if record identifier exists
		query = "SELECT id FROM records WHERE identifier=%s"
		c.execute(query, (self.identifier))
		row = c.fetchone()

		# record
		if row:
			record_id = row[0]
			query = "UPDATE records SET title=%s, description=%s, aggregationlevel=%s WHERE id=%s"
			c.execute(query,(self.title,self.description,self.aggregationlvl,record_id))
			self.deleteExtraData(record_id)
		else:
			query = "INSERT INTO records (identifier,title,description,aggregationlevel) VALUES(%s,%s,%s,%s)"
			c.execute(query,(self.identifier,self.title,self.description,self.aggregationlvl))
			record_id = c.lastrowid

		# extra recorddata insert
		for keyword in self.keywords:
			if keyword:
				keyword_id = database.getKeywordId(self.DB,keyword)
				query = "INSERT INTO record_has_keywords (record_id,keyword_id) VALUES (%s,%s)"
				c.execute(query,(record_id,keyword_id))

		for discipline in self.disciplines:
			if discipline and self.re_uuid.match(discipline):
				discipline_bin_id = self.re_dash.sub("",discipline)
				query = "INSERT INTO record_has_disciplines (record_id,discipline_id_bin) VALUES (%s,UNHEX(%s))"
				c.execute(query,(record_id,discipline_bin_id))

		for educationallevel in self.educationallevels:
			if educationallevel and self.re_uuid.match(educationallevel):
				educationallevel_bin_id = self.re_dash.sub("",educationallevel)
				query = "INSERT INTO record_has_educationallevels (record_id,educationallevel_id_bin) VALUES (%s,UNHEX(%s))"
				c.execute(query,(record_id,educationallevel_bin_id))

		c.close()


	def deleteData(self):
		c = self.DB.cursor()
		c.execute('SELECT id FROM records WHERE identifier=?', i)
		row = c.fetchone()

		if row:
			record_id = row[0]
			query = "DELETE FROM records WHERE id=%s"
			c.execute(query,(record_id))
			self.deleteExtraData(record_id)

		c.close()


	def deleteExtraData(self,record_id):
		c = self.DB.cursor()
		query = "DELETE FROM record_has_keywords WHERE record_id=%s"
		c.execute(query,(record_id))
		query = "DELETE FROM record_has_disciplines WHERE record_id=%s"
		c.execute(query,(record_id))
		query = "DELETE FROM record_has_educationallevels WHERE record_id=%s"
		c.execute(query,(record_id))
