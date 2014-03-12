#!/usr/bin/env python
# coding=utf8

import random, sys, os, json, argparse, math
from termcolor import colored

parser = argparse.ArgumentParser(description='Process.')

parser.add_argument('--results', dest='input', action='store', nargs='?', type=argparse.FileType('r'), default=sys.stdin, help='file to read results from (default stdin)')
args = parser.parse_args()

tests = { }
def grade(**kwargs):
	def wrapper(f):
		name = kwargs['name'] if 'name' in kwargs else f.__name__
		weight = kwargs['weight'] if 'weight' in kwargs else 1.0
		def wrapped(results):
			#print("Grading test %s" % (name))
			
			out = f(results)
			out['weight'] = weight
			out['name'] = name
			return out
		tests[name] = wrapped
		return wrapped
	return wrapper

# Check to see a match
def check(expected, got):
	if isinstance(expected, float): return abs(expected - float(got)) < 0.005
	elif isinstance(expected, int): return expected == int(got)
	elif isinstance(expected, str): return expected == str(got)
	elif expected == None: return got == None
	else: raise TypeError()

# Check a list of items with exponential falloff
def checklist(entries, factor=None, weight=1.0):
	n = len(entries)
	# If there's nothing there assume 0 as result
	if n == 0: return 0
	# Calculate the default falloff
	if factor == None: factor = 1.0-1.0/n
	
	errors = [ entry for entry in entries if not check(entry['expected'], entry['got']) ]
	correct = n - len(errors)
	# Do the magic
	grade = (float(correct)/float(n))*(factor**(n - correct))
	return { 'grade': grade, 'correct': correct, 'total': n, 'weight': weight, 'errors': errors }

def aggregate(entries, normalize=True, weight=1.0):
	values = entries.values() if isinstance(entries, dict) else entries 
	total = reduce(lambda s,a: s+a['weight'], values, 0.0)
	grade = reduce(lambda s,a: s+a['grade']*a['weight']/(total if normalize else 1), values, 0)
	return {
		'grade': grade,
		'weight': weight,
		'parts': entries
	}

def single(value, weight=1.0):
	return { 'grade': value, 'weight': weight }

@grade(weight=0.05)
def emptiness(results):
	return checklist(results)

@grade(weight=0.05)
def commands(results):
	return single(1.0)

@grade(weight=0.05)
def simple(results):
	return checklist(results)

@grade(weight=0.1)
def updates(results):
	return checklist(results)

@grade(weight=0.2)
def sources(results):
	return checklist(results)

@grade(weight=0.3)
def sharding(results):
	return aggregate({
		'ratings': checklist(filter(lambda r : r['type'] == 'EXPECT_RATING', results)),
		# Analyze the usage to ensure that they've filled the 75% quota
		'usage': single(min( [ min(entry['result'] / entry['limit'], 1) for entry in results if entry['type'] == 'TOTAL' ] ), weight=0.2),
		# Analyze the usage stddev to ensure reasonable distribution
		'distribution': single(min( [ min(((entry['factor']/entry['result'])),1) for entry in results if entry['type'] == 'STDDEV' ]))
	})

@grade(weight=0.2)
def performance(results):
	return single(min( [min(-math.log(entry['result'],10)/2.7,1) for entry in results if entry['type'] == 'STDDEV' ] ))

results = { }
for line in args.input:
	obj = json.loads(line)
	name = obj['name']
	if (not name in results): results[name] = [ ]
	results[name].append(obj)



final = { 'the-tea-emporium': aggregate({ name: tests[name](results[name]) for name in tests }) }

letters = {
	0.95: 'A+',
	0.9: 'A',
	0.85: 'A-',
	0.8: 'B+',
	0.75: 'B',
	0.7: 'B-',
	0.65: 'C+',
	0.6: 'C',
	0.55: 'C-',
	0.5: 'D',
	0: 'F'
}

colors = {
	0.85: 'green',
	0.70: 'yellow',
	0: 'red'
}

def letter(score):
	keys = sorted(letters.keys(), key=lambda k: -k)
	i = 0;
	while (keys[i] > score): i = i + 1;
	return letters[keys[i]]

def color(score):
	keys = sorted(colors.keys(), key=lambda k: -k)
	i = 0;
	while (keys[i] > score): i = i + 1;
	return colors[keys[i]]

def percent(score):
	return '{:.2%}'.format(score)

def dump(entry, level=0):
	for key,value in entry.items():
		grade = value['grade']
		print('{0:<30} {1:>10} {2:<4}'.format(('  '*level)+('✔' if grade >= 0.5  else '✖')+' '+key+':', percent(grade), colored(letter(grade),color(grade),attrs=[ 'bold' ] if level == 0 else None )))
		if level == 0: print('  ' + '-'*40)
		if ('parts' in value):
			dump(value['parts'], level+1)

def errors(entry, level=0):
	for key,value in entry.items():
		grade = value['grade']
		if grade == 1.0: continue
		
		if 'errors' in value:
			print(key)
			print('--------')
			print(value['errors'])
			print('\n');
		
		if ('parts' in value):
				errors(value['parts'], level+1)

print('\n### GRADING ####\n')
dump(final)
print('\n### ERRORS ####\n')
errors(final)