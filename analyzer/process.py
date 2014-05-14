# -*- coding: utf-8 -*-

import MySQLdb
import codecs

class Process:
    def __init__(self,config,action,option=''):
        self.DB = MySQLdb.connect(host=config["db_host"],user=config["db_user"], passwd=config["db_passwd"],db=config["db_name"],use_unicode=1)
        self.action = action
        self.config = config
        self.output = codecs.open(self.config["outfile"],'w','utf-8')


    def start(self):
        if self.action == "metadata" or self.action == "querylogs":
            self.analyzeDisciplineCounts(self.action)
        else:
            print("Invalid action, try metadata or querylogs")


    def analyzeDisciplineCounts(self,table_ext):
        c = self.DB.cursor()
        query = "SELECT id,keyword FROM keywords"
        c.execute(query)
        kwdata = c.fetchall()

        for row in kwdata:
            keyword_id = row[0]
            keyword = row[1]
            count = self.selectSumKeyword(table_ext,keyword_id)

            if count > int(self.config["treshold_amount"]):
                data = self.selectKeywordCounts(table_ext,keyword_id)
                if len(data) > int(self.config["treshold_spread"]):
                    if data[0][1] > (count / int(self.config["treshold_ratio"])):
                        self.output.write( keyword + "," + str(count) + "," + str(len(data)) + "," + self.makeUuid(data[0][0]) + "\n" )

        c.close()
        self.output.close()


    def selectSumKeyword(self,table_ext,keyword_id):
        c = self.DB.cursor()
        query = "SELECT SUM(keyword_count) AS countsum FROM keyword_discipline_count_%s WHERE keyword_id = %%s" % table_ext
        c.execute(query,(keyword_id))
        row = c.fetchone()
        c.close()

        if row:
            if row[0] == None:
                return 0
            else:
                return row[0]
        else:
            return 0


    def selectKeywordCounts(self,table_ext,keyword_id):
        c = self.DB.cursor()
        query = "SELECT HEX(discipline_id_bin) AS discipline_id_bin, keyword_count FROM keyword_discipline_count_%s \
            WHERE keyword_id = %%s \
            ORDER BY keyword_count DESC" % table_ext
        c.execute(query,(keyword_id))
        data = c.fetchall()
        c.close()
        return data

    def makeUuid(self,uuid_bin):
        return (uuid_bin[0:8] + "-" + uuid_bin[8:12] + "-" + uuid_bin[12:16] + "-" + uuid_bin[16:20] + "-" + uuid_bin[20:32]).lower()

