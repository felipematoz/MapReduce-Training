import mincemeat
import glob
import csv

text_files = glob.glob('data/*')


def file_contents(file_name):
	f = open(file_name)
	try:
		return f.read()
	finally:
		f.close()

def mapfn(k, v):
	from stopwords import allStopWords
	print 'map:::'
	print '%s' % (k)
	for line in v.split():
		for word in line.split():
			if (word not in allStopWords):
				yield word, 1

def reducefn(k, v):
	print 'reduce:::'
	print '%s' % (k)
	return sum(v)

source = dict((file_name, file_contents(file_name)) for file_name in text_files)

s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="12345")

w= csv.writer(open("results.csv", "w"))
for k, v in results.items():
	w.writerow([k, v])