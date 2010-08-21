#!/usr/bin/env python
# encoding: utf-8
"""
hadoopJobHistory.py

Parses hadoop log files (Job Tracker's /logs/history/done/*)

Created by Will Kinney on 2010-08-17.
Copyright (c) Will Kinney. All rights reserved.
Python 2.6.1
"""

import getopt
import sys
import glob
import os
import logging
import logging.config

logging.config.fileConfig("logging.conf")

# create logger
logger = logging.getLogger("hadoopJobHistory")

def main():
	try:
		opts, args = getopt.getopt(sys.argv[1:], "l:c:", ["logDir=", "confDir="])
	except getopt.GetoptError, err:
		print str(err) 
		usage()
		sys.exit(2)
	logDir = None
	confDir = None
	for o, a in opts:
		if o in ("-l", "--logDir"):
			logDir = a
		elif o in ("-c", "--confDir"):
			confDir = a
		else:
			assert False, "unhandled option"
	if (logDir is None or confDir is None):
		usage()
		sys.exit(2)
		
	# conf logging
	
		
	dirList = os.listdir(logDir)
	for fname in dirList:		
		populateMachineTimes(logDir + "/" + fname, confDir)
# TODO conf

# nj-emperor_1281734026113_job_201008131613_1386_conf.xml
CONF_FILE_FORMAT = 'nj-emperor_*_%(jobId)s_conf.xml'
MAPRED_MAPPER_CLASS_KEY_NAMES = 'mapred.mapper.class', 'mapreduce.map.class'


def usage():
	print "Usage: hadoopJobHistory.py\n\t-l,\t--logDir=DIRECTORY\n\t-c,\t--confDir=DIRECTORY"

def populateMachineTimes(jobFName, confDir):	
	jobId, timesForMachine = getMachineTimes(jobFName)
	if (jobId is None):
		return
	# job_201007061619_2873
	logging.debug('jobId: %s', jobId)
	if logger.isEnabledFor(logging.DEBUG):
		logging.debug('times for machine: %s', timesForMachine.items())
	confFName = CONF_FILE_FORMAT % {'jobId': jobId}
	confFiles = glob.glob(confDir + '/' + confFName)
	logging.debug('conf file matches: %s', confFiles)
	confFName = confFiles[0]
	confFile = open(confFName)
	mapredMapperClass = None
	for line in confFile:
		if line.find(MAPRED_MAPPER_CLASS_KEY_NAMES[0]) != -1 or line.find(MAPRED_MAPPER_CLASS_KEY_NAMES[1]) != -1:
			# <property><name>...</name><value>...</value></property>
			valueIndex = line.index('<value>')
			mapredMapperClass = line[line.index('<value>')+len('<value>'):line.index('</value>')]
			break
	if mapredMapperClass is None:
		logging.warn("mapred mapper class wasn't found, most likely job was configured via jar argument. skipping jobId [%s]", jobId)
		return;
	logging.debug('mapred map class: %s', mapredMapperClass)



def getMachineTimes(fname):
	logging.debug('Parsing log file [%s]', fname)
	jobId = ''
	mapTaskStartTimes = dict()
	tasksToMachine = dict()
	mapTaskEndTimes = dict()
	f = open(fname, 'r')
	# Meta VERSION \"1\" .    <- first line
	
	# TODO
	# mapred.mapper.class
	# total job time
	# job total map input bytes
	
	for line in f:
		words = line.split(" ")
		lineType = words[0]
		if lineType == 'Meta':
			continue
		if lineType == 'Job':
			if jobId == '':
				jobId = parseValue(words[1])
			# filter out killed and failed jobs
			if findKeyValue(words, 'JOB_STATUS', 'KILLED'):
				logging.warn("skipping job because job was killed. job file [%s]", fname)
				return None, None
			if findKeyValue(words, 'JOB_STATUS', 'FAILED'):
				logging.warn("skipping job because job failed. job file [%s]", fname)
				return None, None
		# log file has in order start times and finish times
		if lineType == 'MapAttempt' and findKeyValue(words, 'TASK_TYPE', 'MAP'):
			# map attempt start time lines dont have a task_status key and value
			countValueIfHasValueKey(mapTaskStartTimes, words, 'TASK_ATTEMPT_ID', 'START_TIME')
			if findKeyValue(words, 'TASK_STATUS', 'SUCCESS'):
				countKeyValues(tasksToMachine, words, 'TASK_ATTEMPT_ID', 'HOSTNAME')
				countKeyValues(mapTaskEndTimes, words, 'TASK_ATTEMPT_ID', 'FINISH_TIME')	
			# remove failed and killed task attempts
			if findKeyValue(words, 'TASK_STATUS', 'FAILED'):
				findAndRemoveValueForKey(mapTaskStartTimes, words, 'TASK_ATTEMPT_ID')
				findAndRemoveValueForKey(mapTaskEndTimes, words, 'TASK_ATTEMPT_ID')
			if findKeyValue(words, 'TASK_STATUS', 'KILLED'):
				findAndRemoveValueForKey(mapTaskStartTimes, words, 'TASK_ATTEMPT_ID')
				findAndRemoveValueForKey(mapTaskEndTimes, words, 'TASK_ATTEMPT_ID')
				
			
				


	# clean up machine strings	
	for task in tasksToMachine:
		tasksToMachine[task] = tasksToMachine[task].replace("\\.", ".")
	
	# for item in mapTaskStartTimes.items():
	# 	print item		
	#print
	# for item in mapTaskEndTimes.items():
	# 	print item		
	#print
	# for item in tasksToMachine.items():
		# print item		
	times = dict()
	if len(mapTaskStartTimes) != len(mapTaskEndTimes):
		raise Exception("couldn't find matching start and end times for map tasks in log file", fname)
	for key in mapTaskEndTimes.keys():
		diff = long(mapTaskEndTimes[key]) - long(mapTaskStartTimes[key])
		times[key] = diff
	timesForMachine = dict()
	for key in times.keys():
		machineKey = tasksToMachine[key]
		if machineKey not in timesForMachine:
			timesForMachine[machineKey] = []
		timesForMachine[machineKey].append(times[key])
	return jobId, timesForMachine
			
# ignore if not in there
def findAndRemoveValueForKey(map, words, key):
		map.pop(findValueForKey(words, key), None) 
	
def countKeyValues(map, words, mainKey, mainValueKey):
	key = findValueForKey(words, mainKey)
	value = findValueForKey(words, mainValueKey)
	if key is None or value is None:
		raise Exception("Couldn't find [" + mainKey + "] and/or [" + mainValueKey + "] in " + (" ".join(words)))
	map[key] = value 
					
def findValueForKey(words, key):
	for word in words:
		if parseKey(word) == key:
			return parseValue(word)

# KEY="VALUE" 

def findKeyValue(words, key, value):
	for word in words:
		if parseKey(word) == key and parseValue(word) == value:
			return True
	return False	

def countValueIfHasValueKey(map, words, mainKey, mainValueKey):
	for word in words:
		if parseKey(word) == mainValueKey:
			key = findValueForKey(words, mainKey)
			value = findValueForKey(words, mainValueKey)
			map[key] = value 
			return

def parseKey(toParse):
	return toParse[:toParse.find('=')]
def parseValue(toParse):
	eqIndex = toParse.find('=')
	return toParse[eqIndex+2:len(toParse)-1]

if __name__ == '__main__':
	main()

