#-*- coding: utf-8 -*-
import mincemeat
import glob
import unicodecsv as csv

text_files = glob.glob('data/*')

def file_contents(file_name):
	f = open(file_name)
	try:
		return f.read()
	finally:
		f.close()

def mapfn(k, v):
	from stopwords import allStopWords
	
	for line in v.splitlines():
		tmp = line.split(":::")
		for author in tmp[1].split("::"):
			for word in tmp[2].split():
				if word not in allStopWords and len(word) > 2:
					yield author+":"+word.lower().replace(".", "").replace(",", ""), 1


def reducefn(k, v):
	return sum(v)

source = dict((file_name, file_contents(file_name)) for file_name in text_files)


s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="12345")

w = csv.writer(open("results.csv", "w"))
for k, v in results.items():
	tmp = k.split(":")
	w.writerow([tmp[0], tmp[1], v])
