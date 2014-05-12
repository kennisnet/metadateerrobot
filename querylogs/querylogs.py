# -*- coding: utf-8 -*-

from common import common, database
import MySQLdb
import os
import re
import urllib
from cqlparser import parseString


class ParseQueryLog:
    def __init__(self,config,logfilepath):
        self.DB = MySQLdb.connect(host=config["db_host"],user=config["db_user"], passwd=config["db_passwd"],db=config["db_name"],use_unicode=1)
        self.logfilepath = logfilepath
        self.re_uuid = re.compile(common.getRe('uuid'), re.I)
        self.re_dash = re.compile(common.getRe('dash'))

        self.processLogfiles()


    def processLogfiles(self):      
        for root,dirs,files in os.walk(self.logfilepath):
            for file in files:
                if file.endswith(".log"):
                    print file
                    f=open(os.path.join(root,file), 'r')
                    for line in f:
                        if not 'smdBroker' in line:              
                            queryLineSplitter = QueryLogLineSplitter()
                            query = queryLineSplitter.splitLine(line)
                            
                            if query:
                                cqlstring = ''
                                try:
                                    cqlstring = parseString(query)
                                except:
                                    print("Error in CQL parsing: %s" % query)
                                else:
                                    termExtractor = TermExtractor()
                                    termExtractor.extractTerms(cqlstring)

                                    uniqueTerms = []
                                    for term in termExtractor.returnTerms:
                                        term = term.replace('~', '').replace('*', '').replace('"','').replace('_','')
                                        if term not in uniqueTerms:
                                            uniqueTerms.append(term)

                                    uniqueDisciplines = []
                                    for discipline in termExtractor.discipline:
                                        if discipline not in uniqueDisciplines:
                                            uniqueDisciplines.append(discipline)
                                    
                                    for uniqueTerm in uniqueTerms:
                                        for uniqueDiscipline in uniqueDisciplines:
                                            self.storeData(uniqueTerm, uniqueDiscipline)
    
                                           

                    f.close()


    def storeData(self, keyword, discipline):
        c = self.DB.cursor()
        keyword_id = database.getKeywordId(self.DB,keyword)

        if self.re_uuid.match(discipline):
            discipline_bin_id = self.re_dash.sub("",discipline)
            query = "SELECT keyword_count FROM keyword_discipline_count_querylogs WHERE keyword_id = %s AND discipline_id_bin = UNHEX(%s);"
            c.execute(query,(keyword_id,discipline_bin_id))
            row = c.fetchone()

            if row:
                keyword_count = int(row[0])
                keyword_count += 1
                query = "UPDATE keyword_discipline_count_querylogs SET keyword_count=%s WHERE keyword_id = %s AND discipline_id_bin = UNHEX(%s);"
                c.execute(query,(keyword_count,keyword_id,discipline_bin_id))
            else:
                query = "INSERT INTO keyword_discipline_count_querylogs (keyword_id,discipline_id_bin,keyword_count) VALUES (%s,UNHEX(%s),1);"
                c.execute(query,(keyword_id,discipline_bin_id))

        c.close()



class QueryLogLineSplitter:
    """ Splits a line from the Edurep query log """
    def __init__(self):
        self.linestring = ''
        self.rawquery = ''
        self.query = ''

    def splitLine(self,linestring):
        """ Returns the raw query from a line from the Edurep query log """
        import re

        self.linestring = linestring

        matchObj = re.match( r'([\d|-]+)T([\d|:]+)Z ([\d|\.]+) ([\d|\.]+)K ([\d|\.]+)s (\d*)[hits-]* [\w|\/]* (.*)', self.linestring)

        if matchObj:
            if matchObj.group(7):
                self.rawquery = matchObj.group(7)

        for qarg in self.rawquery.split('&'):
            if qarg.startswith("query="):
                self.query = urllib.unquote(qarg[6:]).replace('+',' ')

        return self.query




class TermExtractor:
    """ Extracts search terms from an Edurep query """
    def __init__(self):
        self.returnTerms = []
        self.discipline = []
        self.saveterm = False
        self.combinedterm = ''
        self.index = ''
        self.boolean = ''

        self.result = []

    def extractTerms(self,cqltree):
        classname = cqltree.__class__.__name__

        if classname == "INDEX":
            self.index = str(cqltree.children[0])
            self.boolean = ''
            self.saveterm = False

        if classname == "SEARCH_CLAUSE":
            self.saveterm = True

        if classname == "SEARCH_TERM":
            self.saveterm = True

        if classname == "BOOLEAN":
            self.boolean = str(cqltree.children[0])

        if classname == "TERM" and self.saveterm:
            if self.index == "" or "general.title" in self.index or "general.keyword" in self.index or "general.description" in self.index:
                if "and" in self.boolean:
                    newTerm = self.returnTerms[-1]+" "+cqltree.children[0]
                    self.returnTerms[-1] = newTerm
                else:
                    self.returnTerms.append( cqltree.children[0] )

            if "discipline.id" in self.index:
                self.discipline.append( cqltree.children[0].replace('(','').replace(')','').replace('"',''))

        for child in cqltree.children:
            if type(child) != str:
                self.extractTerms( child )
