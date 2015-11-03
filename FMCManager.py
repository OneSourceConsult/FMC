#   Copyright (c) 2013-2015, OneSource, Portugal.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

__author__ = "Vitor Fonseca"
__copyright__ = "Copyright (c) 2013-2015, Mobile Cloud Networking (MCN) project"
__credits__ = ["Vitor Fonseca"]
__license__ = "Apache"
__version__ = "1.0"
__maintainer__ = "Vitor Fonseca"
__email__ = "fonseca@onesource.pt"
__status__ = "Production"

"""
FMCManager with WebService
Version 1.0
"""

import socket
import json
import threading
import psycopg2
import string
import datetime
import calendar
import requests
import time

from flask import Flask, jsonify, abort, request

from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

import rpy2.robjects as ro
import rpy2.robjects.numpy2ri as rpyn

r = ro.r

garbage = r.source("METH_BSousa_v10.r")
MADM = r["FMC_MADM"]

port = 50000
shouldRun = True
dbname = "monitoring"
dbuser = "postgres"
dbpass = ""
dbaddress = "127.0.0.1"

confs = {}
mobaas_timer = None


MIGRATION_INDEX = 0
## demo variables
migrationId_demo = MIGRATION_INDEX
newInfo_demo = False
currentAction_demo = None
originCells_demo = None
movingUsers_demo = None
destinationCell_demo = None
destinationUsers_demo = None
migrationData_demo = None
migrating_demo = False
migrationStart_demo = None
migrationEnd_demo = None


class WebServer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.ioLoop = IOLoop.instance()
        self.app = Flask(__name__)
        self.app.add_url_rule(rule='/icnaas/api/v1.0/fmc_manager', methods=['POST'], view_func=self.groupMigration)
        self.app.add_url_rule(rule='/icnaas/api/v1.0/fmc_demo_info', methods=['POST'], view_func=self.send_information)
        self.app.add_url_rule(rule='/icnaas/api/v1.0/fmc_register_mobaas', methods=['POST'], view_func=self.registerMobaas)

    # @self.app.route('/icnaas/api/v1.0/fmc_manager', methods=['POST'])
    def groupMigration(self):
        if not request.json or not 'mobility_prediction' in request.json:
            abort(400)

        print "Received Migration from MP_MIDDLEWARE"
        # print json.loads(request.data)

        mig = Migration()
        code = mig.processMigrationInfo(request.json)

        return jsonify({'result': code}), code
        # return jsonify({'result': 200}), 200

    def send_information(self):

        if not request.json or not 'fmc_info_request' in request.json:
            abort(400)

        global migrationId_demo, currentAction_demo, originCells_demo, movingUsers_demo, destinationCell_demo, destinationUsers_demo, migrationData_demo, migrating_demo, migrationStart_demo, migrationEnd_demo, newInfo_demo

        msg = jsonify({'migrationId': migrationId_demo,
                       'currentAction': currentAction_demo,
                       'originCells': originCells_demo,
                       'movingUsers': movingUsers_demo,
                       'destinationCell': destinationCell_demo,
                       'destinationUsers': destinationUsers_demo,
                       'migrationData': migrationData_demo,
                       'migrating': migrating_demo,
                       'migrationStart': migrationStart_demo,
                       'migrationEnd': migrationEnd_demo,
                       'newInfo': newInfo_demo})

        newInfo_demo = False

        return msg, 200

    def registerMobaas(self):

        if not request.json or not 'fmc_register_mobaas' in request.json:
            abort(400)

        msg = request.json['fmc_register_mobaas']

        url = "http://" + confs["mobaas_ip"] + ":5000/mobaas/api/v1.0/prediction/multi-user"
        resp = requests.post(url, json={"user_id": 0, "future_time_delta": confs["mobaas_delta"],
                                    "current_time": msg['time'],
                                    "current_date": msg['week_day'],
                                    "reply_url": "http://" + confs[
                                        "mp_middleware_ip"] + ":6000/icnaas/api/v1.0/multiple_user_predictions"},
                         timeout=10)

        return resp.text, 200

    def stop(self):
        self.ioLoop.stop()

    def run(self):
        http_server = HTTPServer(WSGIContainer(self.app))
        http_server.listen(5000)
        self.ioLoop.start()


class Monitor(threading.Thread):
    def __init__(self, portNumber):
        threading.Thread.__init__(self)
        self.portNumber = portNumber
        self.soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.soc.bind(("", self.portNumber))

    def sendReply(self, con, code):
        repMsg = dict(type='reply', names=list(), code=code)

        jMsg = json.dumps(repMsg)

        try:
            con.sendall(chr(0xFE))
            con.sendall(bytes(jMsg))
            con.sendall(chr(0xFF))
        except:
            print "Failed while sending Reply Message"

    def readMessage(self, soc):
        try:
            recStr = ""
            beg = 0

            read = soc.recv(1024)
            while read[beg] != chr(0xFE):
                if beg == 1024:
                    read = soc.recv(1024)
                    beg = 0
                else:
                    beg += 1

            while True:
                if read[beg] != chr(0xFE):
                    beg = -1

                if read[-1] == chr(0xFF):
                    recStr += read[beg + 1:-1]
                else:
                    recStr += read[beg + 1:]

                if read[-1] == chr(0xFF):
                    break

                read = soc.recv(1024)

            if len(recStr) > 2:
                return recStr
            else:
                print "Failed while reading Message"
                return None

        except:
            print "Failed while reading Message"
            return None

    def run(self):
        self.soc.settimeout(10)
        self.soc.listen(10)
        global shouldRun
        while shouldRun:
            try:
                conn, addr = self.soc.accept()
            except socket.timeout:
                continue

            incMsg = self.readMessage(conn)

            if incMsg is None:
                self.sendReply(conn, 500)
                conn.close()
                continue

            jMsg = json.loads(incMsg)

            if jMsg["type"] != 'interestlogmsg':
                print "Wrong type of message received: " + jMsg[type]
                self.sendReply(conn, 500)
                conn.close()
                continue

            dbcon = None
            try:
                dbcon = psycopg2.connect(database=dbname, user=dbuser, password=dbpass, host=dbaddress)
                dbcur = dbcon.cursor()

                dbcur.execute("INSERT INTO routers(ip, location, edge, repository)"
                              "(SELECT %s, %s, %s, %s WHERE NOT EXISTS "
                              "(SELECT 1 FROM routers WHERE ip = %s))",
                              (addr[0], jMsg['location'], jMsg['edge'], jMsg['repository'], addr[0]))

                for log in jMsg["entries"]:

                    elements = log.split(';')

                    timestamp = datetime.datetime.strptime(elements[0], "%Y%m%d%H%M%S")
                    prefixLong = elements[1]
                    usedCache = elements[2]
                    totalCache = elements[3]

                    prefixEle = prefixLong.split('=')

                    prefix = prefixEle[0]
                    ver = 'FD'
                    last = ''

                    if (len(prefixEle) == 3):
                        ver = prefixEle[1]
                        last = prefixEle[2]
                    else:
                        last = prefixEle[1]

                    dbcur.execute("INSERT INTO interests(prefix, timestp, router, version, lastblock)"
                                  "VALUES (%s, %s, %s, %s, %s)", (prefix[:-1], timestamp, addr[0], ver[2:-1], last))

                dbcur.execute("UPDATE routers SET cachesize = %s, cacheused = %s "
                              "WHERE ip = %s", (totalCache, usedCache, addr[0]))

                dbcon.commit()
                dbcon.close()

            except:
                print "Failed connection to the database"

                if dbcon:
                    dbcon.rollback()
                    dbcon.close()

                self.sendReply(conn, 500)
                conn.close()
                continue

            self.sendReply(conn, 200)
            conn.close()


class Migration():
    def readMessage(self, soc):
        try:
            recStr = ""
            beg = 0

            read = soc.recv(1024)
            while read[beg] != chr(0xFE):
                if beg == 1024:
                    read = soc.recv(1024)
                    beg = 0
                else:
                    beg += 1

            while True:
                if read[beg] != chr(0xFE):
                    beg = -1

                if read[-1] == chr(0xFF):
                    recStr += read[beg + 1:-1]
                else:
                    recStr += read[beg + 1:]

                if read[-1] == chr(0xFF):
                    break

                read = soc.recv(1024)

            if len(recStr) > 2:
                return recStr
            else:
                print "Failed while reading Message"
                return None

        except:
            print "Failed while reading Message"
            return None

    def getCacheSize(self, ipD, ipR):
        dbcon = None

        print "Get Cache Size"

        try:
            dbcon = psycopg2.connect(database=dbname, user=dbuser, password=dbpass, host=dbaddress)
            dbcur = dbcon.cursor()

            dbcur.execute(
                'SELECT cachesize FROM routers WHERE ip = %s',
                (ipD,))

            cachesize = dbcur.fetchone()

            if not cachesize:
                dbcur.execute(
                    'SELECT cachesize FROM routers WHERE ip = %s',
                    (ipR,))

                cachesize = dbcur.fetchone()

            return int(cachesize[0])

        except Exception as e:
            print "Failed connection to the database while getting cachesize"
            print e

            if dbcon:
                dbcon.rollback()
                dbcon.close()

            return None

    def getInterests(self, ip, timeInterval):
        dbcon = None

        print "Get Interest List"

        try:
            dbcon = psycopg2.connect(database=dbname, user=dbuser, password=dbpass, host=dbaddress)
            dbcur = dbcon.cursor()

            dbcur.execute(
                'SELECT prefix, encode(lastblock, \'escape\') AS "size", COUNT(prefix) AS "count" '
                'From interests '
                'WHERE timestp >= (clock_timestamp() - interval %s) AND router = %s '
                'GROUP BY prefix, size ORDER BY count DESC',
                (timeInterval, ip))

            rows = dbcur.fetchall()

            # Create dictionary
            dInt = {a: [int(b, 16), int(c)] for a, b, c in rows}

            return dInt

        except Exception as e:
            print "Failed connection to the database while getting interests"
            print e

            if dbcon:
                dbcon.rollback()
                dbcon.close()

            return None

    def getMergedInterests(self, ips, timeInterval):

        dSrcs = []

        for ip in ips:
            dSrcs.append(self.getInterests(ip, timeInterval))

        lKeys = []

        for d in dSrcs:
            lKeys.extend(d.keys())

        fDict = dict()
        for key in lKeys:
            for d in dSrcs:
                if key in fDict:
                    fDict[key] += d.get(key, 0)
                else:
                    fDict[key] = d.get(key, 0)

        return fDict

    def getCellRouters(self, cellId):

        url = "http://" + confs["icnaas_ip"] + ":5000/icnaas/api/v1.0/routers/cell/" + str(cellId)

        out = requests.get(url, timeout=10)

        routers = json.loads(out.content)["routers"]

        if len(routers) < 1:
            return None

        ips = []

        for router in routers:
            ips.append(router["public_ip"])

        return ips

    def doMigration(self, ipSrc, movUsers, ipDst, dstUsers):

        cacheSize = self.getCacheSize(ipDst[0], ipSrc[0][0])

        dIntSrc = []

        for src in range(len(ipSrc)):
            dIntSrc.append(self.getMergedInterests(ipSrc[src], "1 months"))

        dIntDst = self.getMergedInterests(ipDst, "1 months")

        # Aux Lists
        auxB = []
        auxC = []
        auxL = []
        auxS = []

        numCells = len(dIntSrc) + 1

        # need mapping (-1)
        cnt = 1

        print "Processing 1st Source Interests"

        # Adding prefixes requested at 1st source
        for k, v in dIntSrc[0].items():
            popDst = dIntDst[k][1] if k in dIntDst else 0

            popSrcs = 0
            for i in range(numCells - 1):
                popSrcs += (dIntSrc[i][k][1] if k in dIntSrc[i] else 0) * movUsers[i]

            popInt = (popSrcs + popDst * dstUsers) / numCells

            ### ID | Populatiry
            auxB.extend([cnt, popInt])

            ### ID | File Size
            auxC.extend([cnt, v[0]])

            auxL.append(k)
            auxS.append(v[0])

            cnt += 1

        print "Processing other Sources Interests"

        for j in range(1, numCells - 1):
            for k, v in dIntSrc[j].items():

                if k not in auxL:
                    popSrcs += (dIntSrc[i][k][1] if k in dIntSrc[i] else 0) * movUsers[i]

                    popSrcs = 0
                    for i in range(j, numCells - 1):
                        popSrcs += (dIntSrc[i][k][1] if k in dIntSrc[i] else 0) * movUsers[i]

                    popInt = (popSrcs + popDst * dstUsers) / numCells

                    ### ID | Populatiry
                    auxB.extend([cnt, popInt])

                    ### ID | File Size
                    auxC.extend([cnt, v[0]])

                    auxL.append(k)
                    auxS.append(v[0])

                    cnt += 1

        print "Processing Destination Interests"

        # Adding interests requested at destination still missing
        for k, v in dIntDst.items():
            if k not in auxL:
                popInt = (v[1] * dstUsers) / numCells

                ### ID | Populatiry
                auxB.extend([cnt, popInt])

                ### ID | File Size
                auxC.extend([cnt, v[0]])

                auxL.append(k)
                auxS.append(v[0])

                cnt += 1

        print "Going to run MADM"

        ## Create matrix Ben and matrix Cost
        mBen = r.matrix(auxB, ncol=2, byrow=True)
        mCost = r.matrix(auxC, ncol=2, byrow=True)

        vBen = ro.FloatVector([1.0])
        vCost = ro.FloatVector([1.0])

        output = MADM(mBen, mCost, vBen, vCost, 1)

        if output == -1:
            return 200

        # print output

        outAux = rpyn.ri2numpy(output)

        if outAux[0] == -2:
            return 200
        elif outAux[0] == -1:
            ####### DEMO INFORMATION #######
            global newInfo_demo, migrationData_demo
            newInfo_demo = True
            migrationData_demo = auxL
            code = self.sendMigrationData(auxL, ipDst)
            return code

        cacheList = []
        usedCache = 0

        for out in outAux:
            if auxS[int(out[0]) - 1] + usedCache < cacheSize:
                cacheList.append(auxL[int(out[0] - 1)])
                usedCache += auxS[int(out[0] - 1)]

            if usedCache == cacheSize:
                break

                # for ent in cacheList:
                # print ent

        ####### DEMO INFORMATION #######
        global newInfo_demo, migrationData_demo
        newInfo_demo = True
        migrationData_demo = cacheList

        code = self.sendMigrationData(cacheList, ipDst)

        return code

    def sendMigrationData(self, prefixList, ipDst):

        cacheMsg = dict(type='populatecachemsg', names=list(), unversioned=False, enumerate=False, timeout=-1)
        cacheMsg["names"] = prefixList

        cnt = 0

        ####### DEMO INFORMATION #######
        global newInfo_demo, migrating_demo, migrationStart_demo, migrationEnd_demo
        newInfo_demo = True
        migrating_demo = True
        migrationStart_demo = time.time()

        for ip in ipDst:
            while cnt < 3:
                jsonMsg = json.dumps(cacheMsg)

                try:
                    print "Trying to send migration data"
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                    s.connect((ip, 50000))

                    s.sendall(chr(0xFE))
                    s.sendall(bytes(jsonMsg))
                    s.sendall(chr(0xFF))

                    respMsg = self.readMessage(s)
                    if respMsg == None:
                        s.close()
                        cnt += 1
                        continue

                    respMsg = json.loads(respMsg)

                    if int(respMsg['code']) < 400:
                        s.close()

                        ####### DEMO INFORMATION #######
                        newInfo_demo = True
                        migrating_demo = False
                        migrationEnd_demo = time.time()

                        return respMsg['code']

                    else:
                        s.close()
                        cnt += 1

                except:
                    s.close()
                    cnt += 1

    def processMigrationInfo(self, msg):
        migration = msg['mobility_prediction']

        cellDst = migration['dest_cell']
        usersDst = int(migration['users'])

        cellsSrc = []
        usersSrc = []

        for ent in migration['origins']:
            cellsSrc.append(ent['cell_id'])
            usersSrc.append(ent['users'])

        # Fetch IPs from ICNaaS based on CellIDs
        ipsSrc = []

        for cell in cellsSrc:
            ips = self.getCellRouters(cell)
            if ips is not None:
                ipsSrc.append(ips)

        code = 400

        ipsDst = self.getCellRouters(cellDst)
        if ipsDst is not None:

            ####### DEMO INFORMATION #######
            global migrationId_demo, newInfo_demo, originCells_demo, movingUsers_demo, destinationCell_demo, destinationUsers_demo
            migrationId_demo += 1
            newInfo_demo = True
            originCells_demo = cellsSrc
            movingUsers_demo = usersSrc
            destinationCell_demo = cellDst
            destinationUsers_demo = usersDst


            code = self.doMigration(ipsSrc, usersSrc, ipsDst, usersDst)

        return code


def mainFunc(webThread):
    while True:

        try:
            op = int(raw_input("CCNManager\nMenu:\n1. Try migration \n0. Exit\nOption: "))
        except:
            continue

        if op == 0:
            global shouldRun
            shouldRun = False
            webThread.stop()
            if mobaas_timer is not None:
                mobaas_timer.cancel()
            break

        if op == 1:
            sIpAddress = raw_input("Src IP Address: ")
            dIpAddress = raw_input("Dst IP Address: ")

            movUsers = int(raw_input("Moving users: "))
            dstUsers = int(raw_input("Destination users: "))

            mig = Migration();

            mig.doMigration([sIpAddress], [movUsers], dIpAddress, dstUsers)


def registerMOBaaS(first=False):
    if first:
        now = datetime.datetime.now()
        tomorrow = datetime.datetime(now.year, now.month, now.day) + datetime.timedelta(1)

        time = abs(tomorrow - now).seconds

        mobaas_timer = threading.Timer(time, registerMOBaaS)
        mobaas_timer.start()

    mobaas_timer = threading.Timer(86400, registerMOBaaS)
    mobaas_timer.start()

    url = "http://" + confs["mobaas_ip"] + ":5000/mobaas/api/v1.0/prediction/multi-user"
    current_time = datetime.datetime.now().strftime('%H:%M')

    resp = requests.post(url, json={"user_id": 0, "future_time_delta": confs["mobaas_delta"],
                                    "current_time": current_time,
                                    "current_date": calendar.day_name[datetime.date.today().weekday()],
                                    "reply_url": "http://" + confs[
                                        "mp_middleware_ip"] + ":6000/icnaas/api/v1.0/multiple_user_predictions"},
                         timeout=10)

    print resp.text


if __name__ == '__main__':

    with open('/home/ubuntu/provisioning_FMC.conf', 'r') as opts:
        for line in opts.readlines():
            confs[line.split('=')[0]] = line.split('=')[1][:-1]

    monThread = Monitor(port + 5)
    monThread.start()

    webThread = WebServer()
    webThread.start()

    #threading.Timer(5, registerMOBaaS, [True]).start()

    # mainFunc(webThread)
