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
    for line in v.splitlines():
        for author in line.split(':::')[1].split('::'):
            yield author, line.split(':::')[2]

def reducefn(k, v):
    print 'reduce ' + k
    total = 0
    nomeFilial = ''
    for index, item in enumerate(v):
        if item.split(':')[0] == 'Vendas':
            total = int(item.split(':')[1]) + total
        if item.split(':')[0] == 'Filial':
            nomeFilial = item.split(':')[1]
    L = list()
    L.append(nomeFilial + ', ' + str(total))
    return L

s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="p4ssw0rd")

w = csv.writer(open("C:\\Temp\\Author\\result.csv", "w"))
for k, v in results.items():
    w.writerow([k, str(v).replace("[", "").replace("]", "").replace("'", "").replace(" ", "")])