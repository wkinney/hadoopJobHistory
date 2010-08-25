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
	if logDir is None or confDir is None:
		usage()
		sys.exit(2)
	printJobHistory(logDir, confDir)

def usage():
	print "Usage: hadoopJobHistory.py\n\t-l,\t--logDir=DIRECTORY\n\t-c,\t--confDir=DIRECTORY"

# e.g. <hostname>_1281734026113_job_201008131613_1386_conf.xml
CONF_FILE_FORMAT = '*_%(jobId)s_conf.xml'
MAPRED_MAPPER_CLASS_KEY_NAMES = 'mapred.mapper.class', 'mapreduce.map.class'

def printJobHistory(logDir, confDir):
	# Jobs: jobId, mapClass, jobLaunchTime
	# Tasks: jobId, mapClass, jobLaunchTime, taskAttemptId, machineName, taskCompletionTime
	jobs = []
	tasks = []
	dirList = os.listdir(logDir)
	for fname in dirList:
		populateJobDetails(os.path.join(logDir, fname), confDir, jobs, tasks)
			
	#outputTotalMachinePerformanceNormalizedByMapClass(tasks)
	print 'jobId,mapClass,jobLaunchTime,taskAttemptId,machineName,taskCompletionTime'
	for task in tasks:
		print ','.join(map(str, task))
		

def cleanMachineString(machine):
	pass
		
def outputTotalMachinePerformanceNormalizedByMapClass(tasks):
	avgMachineForJobTaskTimes = dict()
	avgMachineTaskTimes = dict()
	maxAvgJobClassTaskTimes = dict()
	for task in tasks:
		key = tuple([task[1], long(cleanMachineString(task[4]))])
		if key not in avgMachineForJobTaskTimes:
			avgMachineForJobTaskTimes[key] = []
		avgMachineForJobTaskTimes[key].append(task[5])
	for jobMachineKey in avgMachineForJobTaskTimes.keys():
		avg = float(sum(avgMachineForJobTaskTimes[jobMachineKey])) / float(len(avgMachineForJobTaskTimes[jobMachineKey]))
		avgMachineForJobTaskTimes[jobMachineKey] = avg;
		# map maxes for map classes
		if jobMachineKey[0] not in maxAvgJobClassTaskTimes:
			maxAvgJobClassTaskTimes[jobMachineKey[0]] = avg
		elif avg > maxAvgJobClassTaskTimes[jobMachineKey[0]]:
			maxAvgJobClassTaskTimes[jobMachineKey[0]] = avg
	# normalize times
	for jobMachineKey in avgMachineForJobTaskTimes.keys():
		normalizedAvg = avgMachineForJobTaskTimes[jobMachineKey] / float(maxAvgJobClassTaskTimes[jobMachineKey[0]])
		avgMachineForJobTaskTimes[jobMachineKey] = normalizedAvg * 100
	
	for jobMachineKey in avgMachineForJobTaskTimes.keys():
		if jobMachineKey[1] not in avgMachineTaskTimes:
			avgMachineTaskTimes[jobMachineKey[1]] = []
		avgMachineTaskTimes[jobMachineKey[1]].append(avgMachineForJobTaskTimes[jobMachineKey])
		
	for machine in avgMachineTaskTimes.keys():
		avgMachineTime = sum(avgMachineTaskTimes[machine]) / float(len(avgMachineTaskTimes[machine]))
		avgMachineTaskTimes[machine] = avgMachineTime
		
	machineKeys = avgMachineTaskTimes.keys()
	machineKeys.sort()
	print 'machine\tnormalizedAverageTaskTime'
	for machine in machineKeys:
		print str(machine) + '\t' + str(avgMachineTaskTimes[machine])
		

def populateJobDetails(jobFName, confDir, jobs, tasks):
	jobId, launchTime, tasksToTimes, tasksToMachine = getJobDetails(jobFName)
	if (jobId is None):
		return
	# e.g. job_201007061619_2873
	logging.debug('jobId: %s', jobId)
	if logger.isEnabledFor(logging.DEBUG):
		logging.debug('tasks to times: %s', tasksToTimes.items())
	confFName = CONF_FILE_FORMAT % {'jobId': jobId}
	confFiles = glob.glob(os.path.join(confDir,confFName))
	logging.debug('conf file matches: %s', confFiles)
	confFName = confFiles[0]
	mapClass = parseJobMapClass(confFName)
	if mapClass is None:
		logging.warn("map class wasn't found, most likely job was configured via jar argument. skipping file [%s]", jobFName)
		return;
	logging.debug('mapred map class: %s', mapClass)
	# add job to jobs list
	jobs.append(tuple([jobId, mapClass, launchTime]))
	# populate task details
	for task in tasksToTimes.keys():
		tasks.append(tuple([jobId, mapClass, launchTime, task, tasksToMachine[task], tasksToTimes[task]]))
		

def parseJobMapClass(confFName):
	for line in open(confFName):
		if line.find(MAPRED_MAPPER_CLASS_KEY_NAMES[0]) != -1 or line.find(MAPRED_MAPPER_CLASS_KEY_NAMES[1]) != -1:
			# <property><name>...</name><value>...</value></property>
			valueIndex = line.index('<value>')
			return line[line.index('<value>')+len('<value>'):line.index('</value>')]

def getJobDetails(fname):
	logging.debug('Parsing log file [%s]', fname)
	jobId = None
	launchTime = None
	mapTaskStartTimes = dict()
	taskToMachine = dict()
	mapTaskEndTimes = dict()
	f = open(fname, 'r')
	# Meta VERSION \"1\" .    <- first line
	
	# TODO
	# total job time
	# job total map input bytes
	
	for line in f:
		words = line.split(" ")
		lineType = words[0]
		if lineType == 'Meta':
			continue
		if lineType == 'Job':
			if jobId is None:
				jobId = parseValue(words[1])
			if launchTime is None:
				launchTime = findValueForKey(words, 'LAUNCH_TIME')
			# filter out killed and failed jobs
			if findKeyValue(words, 'JOB_STATUS', 'KILLED'):
				logging.warn("skipping job because job was killed. job file [%s]", fname)
				return None, None, None, None
			if findKeyValue(words, 'JOB_STATUS', 'FAILED'):
				logging.warn("skipping job because job failed. job file [%s]", fname)
				return None, None, None, None
		# log file has in order start times and finish times
		if lineType == 'MapAttempt' and findKeyValue(words, 'TASK_TYPE', 'MAP'):
			# map attempt start time lines dont have a task_status key and value
			countValueIfHasValueKey(mapTaskStartTimes, words, 'TASK_ATTEMPT_ID', 'START_TIME')
			if findKeyValue(words, 'TASK_STATUS', 'SUCCESS'):
				countKeyValues(taskToMachine, words, 'TASK_ATTEMPT_ID', 'HOSTNAME')
				countKeyValues(mapTaskEndTimes, words, 'TASK_ATTEMPT_ID', 'FINISH_TIME')	
			# remove failed and killed task attempts
			if findKeyValue(words, 'TASK_STATUS', 'FAILED'):
				findAndRemoveValueForKey(mapTaskStartTimes, words, 'TASK_ATTEMPT_ID')
				findAndRemoveValueForKey(mapTaskEndTimes, words, 'TASK_ATTEMPT_ID')
			if findKeyValue(words, 'TASK_STATUS', 'KILLED'):
				findAndRemoveValueForKey(mapTaskStartTimes, words, 'TASK_ATTEMPT_ID')
				findAndRemoveValueForKey(mapTaskEndTimes, words, 'TASK_ATTEMPT_ID')
				
	# clean up machine strings	
	for task in taskToMachine:
		taskToMachine[task] = taskToMachine[task].replace("\\.", ".")

	if len(mapTaskStartTimes) != len(mapTaskEndTimes):
		raise Exception("couldn't find matching start and end times for map tasks in log file", fname)

	taskToTime = dict()
	for key in mapTaskEndTimes.keys():
		diff = long(mapTaskEndTimes[key]) - long(mapTaskStartTimes[key])
		taskToTime[key] = diff

	return jobId, launchTime, taskToTime, taskToMachine
			
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

