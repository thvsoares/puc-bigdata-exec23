import mincemeat
import glob
import csv

data_files = glob.glob('C:\\Temp\\Author\\Data\\*')

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()
    
source = dict((file_name, file_contents(file_name))for file_name in data_files)

def mapfn(k,v):
    print 'map ' + k
    from stopwords import allStopWords
    for line in v.splitlines():
        for author in line.split(':::')[1].split('::'):
            for word in line.split(':::')[2].split():
                if (word not in allStopWords):
                    yield author word

def reducefn(k, v):
    print 'reduce ' + k
    words = dict()
    for word in v:
        words[word] = words[word] + 1
    l = list()
    for k, w in words.items():
        l.append(k + ':' + str(v) + ', ')
    return l

s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="p4ssw0rd")

w = csv.writer(open("C:\\Temp\\Author\\result.csv", "w"))
for k, v in results.items():
    w.writerow([k, str(v)])