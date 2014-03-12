# -*- coding: utf-8 -*-

def getConfig(configfile,section):
	import ConfigParser
	Config = ConfigParser.ConfigParser()
	Config.read(configfile)

	config_options = {}
	options = Config.options(section)
	for option in options:
		try:
			config_options[option] = Config.get(section, option)
			if config_options[option] == -1:
				DebugPrint("skip: %s" % option)
		except:
			print("exception on %s!" % option)
			config_options[option] = None

	return config_options


def getRe(x):
	return {
		'uuid': "[0-F]{8}-[0-F]{4}-[0-F]{4}-[0-F]{4}-[0-F]{12}",
		'dash': "-",
		}.get(x, False)

