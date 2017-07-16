from flask import Flask, request, make_response,render_template
import pymysql  #connect to rds--mysql
import time   #query time
import memcache  #importing memcache
import hashlib #for hashing the queries
import boto3
import os
from random import randint

app = Flask(__name__)
ACCESS_KEY_ID = '***********************'
ACCESS_SECRET_KEY = '******************'
BUCKET_NAME = 'vin21-*************'
FILE_NAME = 'test.jpg';

s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY_ID,
                    aws_secret_access_key=ACCESS_SECRET_KEY,config= boto3.session.Config(signature_version='s3v4'))
client = boto3.client('s3')
resource = boto3.resource('s3')

app.secret_key='any string'
mypath = '**********************'

#credentials to connect to the database
hostname = 'vin21-***************us-east-2.rds.amazonaws.com'
username = '******'
password = '********'
database = 'my_db'
myConnection = pymysql.connect( host=hostname, user=username, passwd=password, db=database,charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor, local_infile=True )
cur = myConnection.cursor()
#connecting to memcache
memc=memcache.Client(['***************.amazonaws.com:11211'])

@app.route('/')
def hello_world():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']
    file_name = file.filename
    content = file.read()
    s3.Bucket('vin21-********').put_object(Key=file_name, Body=content)
    createTableQuery = 'create table data (gender text,givenname text,surname text,streetaddress text,city text,state text,emailaddress text,username text,telephone text,age int,bloodtype text, centimeter int,latitude double, longitude double)'
    cur.execute(createTableQuery)
    myConnection.commit()

    loadQuery = """LOAD DATA LOCAL INFILE '/home/ubuntu/data.csv' INTO TABLE
                                              data FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED
                                              BY '"' Lines terminated by '\n' IGNORE 1 LINES  """
    cur.execute(loadQuery)
    myConnection.commit()
    return render_template('response.html', response = "Successfully uploaded")

@app.route('/total', methods=['POST'])
def totalEntries():
    query1 = 'select count(*) from data'
    cur.execute(query1)
    count = cur.fetchall()
    count = count[0].get('count(*)')
    return render_template('index.html',count=count)


@app.route('/randomQuery_mem', methods=['POST'])
def randomQuery():
    num = request.form['num']
    state = request.form['state']
    state = "'" + state + "'"
    min = request.form['min']
    max = request.form['max']
    result2 = []
    starttime = time.time()
    for i in range(1, int(num)):
        rgen = randint(min, max)
        randomQuery = "select givenname,centimeter,age from data where state = " + str(state) + " and centimeter= " + str(rgen) + " Limit 10"
        randomQueryHash = hashlib.sha256((randomQuery).encode('utf-8')).hexdigest()
        result = memc.get(randomQueryHash)

        print(result)
        if not result:
            print("in if")
            cur.execute(randomQuery)
            value = cur.fetchall()
            memc.set(randomQueryHash, value)
            result2.append(value)
        else:
            result2.append(result)

    endtime = time.time()
    memTime = endtime - starttime
    d = 0
    str2 = " "
    fileList = []
    for row in result2:
        d = d + 1
        fileInfo = {}
        fileInfo['num'] = str(d)
        fileInfo['res'] = str(row)
        fileList.append(fileInfo)


    return render_template('response.html',memTime= str(memTime))

@app.route('/randomQuery_rds', methods=['POST'])
def randomQuery_rds():
    num = request.form['num']
    state = request.form['state']
    state = "'"+state+"'"
    min = request.form['min']
    max = request.form['max']
    cur = myConnection.cursor()
    result2 = []
    starttime = time.time()
    for i in range(1, int(num)):
        rgen = randint(int(min), int(max))
        randomQuery = "select givenname,centimeter,age from data where state = "+state+" and centimeter= "+str(rgen)+" Limit 10"
        cur.execute(randomQuery)

    endtime = time.time()
    rdsTime = endtime - starttime
    return render_template('response.html',rdsTime= str(rdsTime))

@app.route('/randomQuery_rdsTime', methods=['POST'])
def randomQuery_time():
    state = request.form['state']
    min = request.form['min']
    max = request.form['max']
    limit = request.form['limit']
    timeLimit = request.form['timelimit']
    cur = myConnection.cursor()

    starttime = time.time()
    count=0
    for i in range(1, 10000):
        curTime=time.time()-starttime
        if float(curTime)<float(timeLimit):
            count = count + 1
            rgen = randint(1901, 1905)
            randomQuery = 'select status from healthcare where BirthYear=' + str(rgen) + ' Limit ' + limit
            cur.execute(randomQuery)
        else:
            break

    endtime = time.time()
    rdsTotalTime = endtime - starttime
    return render_template('index.html',rdsTimeTotal= str(rdsTotalTime),noOfQueries = count)

@app.route('/randomQuery_memTime', methods=['POST'])
def randomQuery_memtime():
    limit = request.form['limit']
    timeLimit = request.form['timelimit']
    cur = myConnection.cursor()
    starttime = time.time()
    count=0

    for i in range(1, 10000):
        curTime=time.time()-starttime
        if float(curTime)<float(timeLimit):
            rgen = randint(1901, 1905)
            randomQuery = 'select status from healthcare where BirthYear=' + str(rgen) + ' Limit ' + limit
            randomQueryHash = hashlib.sha256((randomQuery).encode('utf-8')).hexdigest()
            result = memc.get(randomQueryHash)
            count = count + 1
            if not result:
                print("in if")
                cur.execute(randomQuery)
                value = cur.fetchall()
                memc.set(randomQueryHash, value)
        else:
            break

    endtime = time.time()
    memTotalTime = endtime - starttime
    return render_template('index.html',memTotalTime= str(memTotalTime),noOfMemQueries = count)




@app.route('/surnameQuery', methods=['POST'])
def surnameQuery():
    starttime = time.time()
    surname = request.form['surname']

    surnameQuery1 = "select givenname,telephone,state from data where surname='%s'" %surname
    cur.execute(surnameQuery1)
    results = cur.fetchall()
    surnameQueryHash = hashlib.sha256((surnameQuery1).encode('utf-8')).hexdigest()
    memc.set(surnameQueryHash, results)
    fileList = []
    d = 0
    for row in results:
        d = d + 1
        fileInfo = {}
        fileInfo['num'] = str(d)
        fileInfo['res'] = str(row)
        fileList.append(fileInfo)
    endtime = time.time()
    rdsTime = endtime - starttime
    return render_template('response.html',files = fileList, rdsTime = rdsTime)

@app.route('/surnameQueryMem', methods=['POST'])
def surnameQueryMem():
    surname = request.form['surname']

    surnameQuery1 = "select givenname,telephone,state from data where surname='%s'" %surname
    surnameQueryHash = hashlib.sha256((surnameQuery1).encode('utf-8')).hexdigest()
    result = memc.get(surnameQueryHash)
    starttime = time.time()
    results = []
    if not result:
        cur.execute(surnameQuery1)
        value = cur.fetchall()
        memc.set(surnameQueryHash, value)
        results.append(value)
    else:
        results.append(result)

    endtime = time.time()
    memTime = endtime - starttime
    fileList = []
    d = 0
    for row in results:
        d = d + 1
        fileInfo = {}
        fileInfo['num'] = str(d)
        fileInfo['res'] = str(row)
        fileList.append(fileInfo)
    return render_template('response.html',files = fileList, memTime = memTime)


@app.route('/query2', methods=['POST'])
def query2():
    starttime = time.time()
    state = request.form['state']
    min = request.form['min']
    max = request.form['max']
    query2 = "select count(*) from data where centimeter between "+min+" and "+max+" and state = '%s'" %state
    cur.execute(query2)
    count = cur.fetchall()
    query2 = "select givenname,city,state,centimeter from data where centimeter between "+min+" and "+max+" and state = '%s' LIMIT 10" %state
    cur.execute(query2)
    results = cur.fetchall()
    fileList = []
    d = 0
    for row in results:
        d = d + 1
        fileInfo = {}
        fileInfo['num'] = str(d)
        fileInfo['res'] = str(row)
        fileList.append(fileInfo)
    endtime = time.time()
    rdsTime = endtime - starttime
    return render_template('response.html',files = fileList, count=count,rdsTime = rdsTime)

@app.route('/query2Mem', methods=['POST'])

def query2Mem():
    state = request.form['state']
    min = request.form['min']
    max = request.form['max']
    query2count = "select count(*) from data where centimeter between "+min+" and "+max+" and state = '%s'" %state
    cur.execute(query2count)
    count = cur.fetchall()
    query2 = "select givenname,city,state,centimeter from data where centimeter between "+min+" and "+max+" and state = '%s'" %state
    query2Hash = hashlib.sha256((query2).encode('utf-8')).hexdigest()
    cur.execute(query2)
    results = cur.fetchall()
    result = memc.get(query2Hash)
    starttime = time.time()

    results = []
    if not result:
        cur.execute(query2)
        value = cur.fetchall()
        memc.set(query2Hash, value)
        results.append(value)
    else:
        results.append(result)


    endtime = time.time()
    memTime = endtime - starttime
    fileList = []
    d = 0
    for row in results:
        d = d + 1
        fileInfo = {}
        fileInfo['num'] = str(d)
        fileInfo['res'] = str(row)
        fileList.append(fileInfo)
    return render_template('response.html',files = fileList,count = count, memTime=memTime)

@app.route('/query3', methods=['POST'])
def query3():
    starttime = time.time()

    min = request.form['min']
    max = request.form['max']
    query2 = "select count(*) from data where centimeter between "+min+" and "+max+" and state = '%s'" %state
    cur.execute(query2)
    count = cur.fetchall()
    query2 = "select givenname,city,state,centimeter from data where centimeter between "+min+" and "+max+" and state = '%s' LIMIT 10" %state
    cur.execute(query2)
    results = cur.fetchall()
    fileList = []
    d = 0
    for row in results:
        d = d + 1
        fileInfo = {}
        fileInfo['num'] = str(d)
        fileInfo['res'] = str(row)
        fileList.append(fileInfo)
    endtime = time.time()
    rdsTime = endtime - starttime
    return render_template('response.html',files = fileList, count=count,rdsTime = rdsTime)


if __name__ == '__main__':
    app.run(debug=True)



