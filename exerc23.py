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

def mapfn(k, v):
    try:
        print 'map ' + k
        from stopwords import allStopWords
        for line in v.splitlines():
            for author in line.split(':::')[1].split('::'):
                for word in line.split(':::')[2].split():
                        word_clear = word.replace(".", "").replace("-", "")
                        if (word_clear and word_clear not in allStopWords):
                            yield author, word_clear
    except:
        print 'map error'

def reducefn(k, v):
    try:
        print 'reduce ' + k
        words = dict()
        for word in v:
            if (words.get(word, None) is None):
                words[word] = 0
            words[word] = words[word] + 1
        l = list()
        for k, w in words.items():
            l.append(k + ':' + str(w) + ', ')
        return l
    except:
        print 'reduce error'
        return None

s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="p4ssw0rd")

w = csv.writer(open("C:\\Temp\\Author\\result.csv", "w"))
for k, v in results.items():
    w.writerow([k, str(v)])