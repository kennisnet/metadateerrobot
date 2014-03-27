# -*- coding: utf-8 -*-

from common import file
import MySQLdb
import re


class Process:
    def __init__(self,config,action,option=''):
        self.DB = MySQLdb.connect(host=config["db_host"],user=config["db_user"], passwd=config["db_passwd"],db=config["db_name"],use_unicode=1)
        self.action = action
        self.config = config


    def start(self):
        if self.action == "updatedb":
            self.updateDb()
        else:
            print("Invalid action, try updatedb")


    def updateDb(self):
        c = self.DB.cursor()
        opentaaldata = file.mapFile(self.config["src_db_path"])
        query = "SELECT id,keyword FROM keywords"
        c.execute(query)
        kwdata = c.fetchall()

        for row in kwdata:
            keyword_id = row[0]
            keyword = row[1]

            try:
                regex = re.compile(r'^' + keyword + '$', re.I|re.MULTILINE)
            except:
                regex = re.compile(r'^zzzzzzz$', re.I|re.MULTILINE)

            if regex.search(opentaaldata):
                query = "UPDATE keywords SET opentaal = 1 WHERE id = %s"
                c.execute(query,(keyword_id))

        c.close()
