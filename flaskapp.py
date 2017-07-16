from flask import Flask, request, make_response,render_template
import pymysql  #connect to rds--mysql
import time   #query time
import memcache  #importing memcache
import hashlib #for hashing the queries
import boto3
import os

app = Flask(__name__)
ACCESS_KEY_ID = '******************'
ACCESS_SECRET_KEY = '*******************'
BUCKET_NAME = 'vin21-bucket'
FILE_NAME = 'test.jpg';

s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY_ID,
                    aws_secret_access_key=ACCESS_SECRET_KEY,config= boto3.session.Config(signature_version='s3v4'))
client = boto3.client('s3')
resource = boto3.resource('s3')

app.secret_key='any string'
mypath = '************************************'
hostname = '******************************************'
username = 'Vinayak'
password = '*******'
database = 'my_db'
myConnection = pymysql.connect( host=hostname, user=username, passwd=password, db=database,charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor, local_infile=True )
cur = myConnection.cursor()

@app.route('/')
def hello_world():
    return render_template('index.html',)

@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']
    file_name = file.filename
    tab_file = file_name.split('.')[0]
    content = file.read()
    s3.Bucket('vin21-bucket').put_object(Key=file_name, Body=content)
    droptablequery = 'DROP TABLE boat'
    cur.execute(droptablequery)
    createTableQuery = 'create table boat (pclass int(10),survived int(10),name text,sex text,age double,ticket int,fair double,cabin text,homedest text)'
    cur.execute(createTableQuery)
    myConnection.commit()

    loadQuery = """LOAD DATA LOCAL INFILE '/home/ubuntu/boat.csv' INTO TABLE
                                          boat FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED
                                          BY '"' Lines terminated by '\n' IGNORE 1 LINES  """
    cur.execute(loadQuery)
    myConnection.commit()
    return render_template('index.html', response = "Successfully uploaded")

@app.route('/total', methods=['POST'])
def totalEntries():
    query1 = 'select count(*) from boat'
    cur.execute(query1)
    count = cur.fetchall()
    count = count[0].get('count(*)')
    return render_template('index.html',count=count)

@app.route('/latitude', methods=['POST'])
def lat():
    latitude = request.form['latitude']
    longi = request.form['longi']
    query1 = 'select count(*) from quakes'
    cur.execute(query1)
    count = cur.fetchall()
    count = count[0].get('count(*)')
    return render_template('index.html',count=count)


@app.route('/query', methods=['POST'])
def query():
    query1 = 'select * from quakes limit 1000'
    q1hash = hashlib.sha256((query1).encode('utf-8')).hexdigest()
    selectQuery = "select * from quakes where dayname(time)='sunday'"
    cur.execute(selectQuery)
    selectResult=cur.fetchall()
    minMagQuery = "select Min(mag) from quakes"
    cur.execute(minMagQuery)
    minMag =cur.fetchall()
    minMag = minMag[0].get('Min(mag)')
    print(minMag)
    maxMagQuery = "select Max(mag) from quakes"
    cur.execute(maxMagQuery)
    maxMag = cur.fetchall()
    maxMag = maxMag[0].get('Max(mag)')
    print(maxMag)
    magRangeQuery = "select * from quakes where mag between %s and %s"% (5, 5.9)
    cur.execute(magRangeQuery)
    range = cur.fetchall()
    print(range)
    cleanQuery = "delete from quakes where gap is null"
    cur.execute(cleanQuery)
    starttime = time.time()
    cur.execute(query1)
    endtime = time.time()
    tt1 = endtime - starttime
    print("time : ", tt1)
    files = []
    fileInfo = {}
    fileInfo['tt1'] = tt1
    result = cur.fetchall()  # contains all the results
    c = 0
    for row in result:
        c = c + 1
        print(str(c) + ':' + str(row))

    print(' 1000 tuples returned from rds query')
    print('rds time taken', tt1)
    starttime = time.time()
    endtime = time.time()
    tt2 = endtime - starttime
    print('1000tuples returned from memcache')
    print('memcache time taken', tt2)
    fileInfo['tt2'] = tt2
    files.append(fileInfo)
    myConnection.commit()
    return render_template('response.html', files=files, slc = selectResult, range = range)

@app.route('/ageQuery', methods=['POST'])
def query2():
    age = request.form['age']
    sex = request.form['sex']

    RangeQuery = "select name from boat where age = %s and sex = '%s'" % (age, sex)
    cur.execute(RangeQuery)
    names = cur.fetchall()
    RangeQuery2 = "select count(*) from boat where age = %s and sex = '%s'" % (age, sex)
    cur.execute(RangeQuery2)
    count = cur.fetchall()
    return render_template('response.html',names = names, cnt = count)
    
@app.route('/ageRangeQuery', methods=['POST'])
def query3():
    minAge = request.form['minAge']
    maxAge = request.form['maxAge']
    RangeQuery = "select name from boat where survived = 1 and age between %s and %s" %(minAge,maxAge)
    cur.execute(RangeQuery)
    names = cur.fetchall()
    return render_template('response.html', srNames=names)

@app.route('/query4', methods=['POST'])
def query4():
    lastname = request.form['lastname']
    lastname2 = '%' + str(lastname) + '%'
    lastname='%'
    RangeQuery = "select * from boat where name like '%s'" %lastname2
    cur.execute(RangeQuery)
    names = cur.fetchall()
    return render_template('response.html', likeQ=names)

@app.route('/fairRangeQuery', methods=['POST'])
def query5():
    minfair = request.form['minfair']
    maxfair = request.form['maxfair']
    lastname='%'
    fairRangeQuery = "select * from boat where fair between %s and %s" % (minfair, maxfair)
    print(fairRangeQuery)
    cur.execute(fairRangeQuery)
    fairRange = cur.fetchall()
    return render_template('response.html', fairRange=fairRange)

@app.route('/latQuery', methods=['POST'])
def latQuery():
    latR = request.form['latR']
    latR = int(latR)
    print(latR+2)
    lt1 = latR-2
    lt2 = latR+2
    lt1 = str(lt1)
    lt2 = str(lt2)
    longiR = request.form['longiR']
    longiR = int(longiR)
    lg1 = longiR-2
    lg2 = longiR+2
    lg1 = str(lg1)
    lg2 = str(lg2)
    print(longiR)
    latLongiQuery = "select * from quakes where latitude between "+lt1+" and "+lt2+" and longitude between "+lg1+" and "+lg2
    cur.execute(latLongiQuery)
    places = cur.fetchall()
    print(places)
    placeRange = []
    c=0
    for row in places:
        c = c + 1
        r = {}
        print(str(c) + ':' + str(row.get('place')))
        r['place'] = str(row.get('place'))
        placeRange.append(r)

    return render_template('response.html',places = placeRange)
if __name__ == '__main__':
    app.run(debug=True)


