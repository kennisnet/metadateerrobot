# -*- coding: utf-8 -*-

import MySQLdb

class Process:
    def __init__(self,config,action,option=''):
        self.DB = MySQLdb.connect(host=config["db_host"],user=config["db_user"], passwd=config["db_passwd"],db=config["db_name"],use_unicode=1)
        self.action = action


    def start(self):
        if self.action == "keyworddisciplinecount":
            self.keywordDisciplineCount()
        else:
            print("Invalid action, try keyworddisciplinecount")


    def keywordDisciplineCount(self):
        c = self.DB.cursor()

        # clean table
        query = "TRUNCATE keyword_discipline_count_metadata"
        c.execute(query)

        # get unique disciplines from record_has_disciplines
        query = "SELECT DISTINCT discipline_id_bin FROM record_has_disciplines"
        c.execute(query)
        data = c.fetchall()

        # insert count for each discipline
        for row in data:
            discipline_id_bin = row[0]

            query = "INSERT INTO keyword_discipline_count_metadata (keyword_id,discipline_id_bin,keyword_count) \
                     SELECT keyword_id, discipline_id_bin, count(*) as keyword_count FROM records \
                     LEFT JOIN record_has_disciplines ON records.id = record_has_disciplines.record_id \
                     LEFT JOIN record_has_keywords ON records.id = record_has_keywords.record_id \
                     WHERE keyword_id IS NOT NULL \
                     AND discipline_id_bin = %s \
                     GROUP BY keyword_id"
            c.execute(query,(discipline_id_bin))

        c.close()
