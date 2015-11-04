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

__author__ = "Vitor Fonseca, Claudio Marques"
__copyright__ = "Copyright (c) 2013-2015, Mobile Cloud Networking (MCN) project"
__credits__ = ["Vitor Fonseca, Claudio Marques"]
__license__ = "Apache"
__version__ = "1.0"
__maintainer__ = "Vitor Fonseca, Claudio Marques "
__email__ = "fonseca@onesource.pt, claudio@onesource.pt"
__status__ = "Production"

"""
RESTful Web Service for Mobility Prediction middleware.
Version 1.0
"""
from flask import Flask, jsonify, request, abort

import requests, json

app = Flask(__name__)


@app.route('/icnaas/api/v1.0/multiple_user_predictions', methods=['POST'])
def handle_multiple_user_predictions():
    if not request.json or not 'multiple_user_predictions' in request.json:
        abort(400)

    print "Received Prediction from MOBaaS"
    process_predictions(request.json)

    return jsonify({'result': True}), 200

def add_mobility_list(mobilities, origin_cells, d_cell, o_cell):

    origin_cells[o_cell] -= 1

    for ele in mobilities:
        if d_cell == ele["dest_cell"]:
            for ori in ele["origins"]:
                if o_cell == ori["cell_id"]:
                    ori["users"] += 1
                    return

                ele["origins"].append({"cell_id":o_cell, "users":1})
                return

    mobilities.append({"dest_cell":d_cell, "users":0, "origins":[{"cell_id":o_cell, "users":1}]})

def process_predictions(mobaas_data):

    #print mobaas_data

    data = mobaas_data['multiple_user_predictions']

    #print "processing data from MOBaaS"

    origin_cells = {}
    mobilities = []

    for ent in data:
        if ent['current_cell_id'] not in origin_cells.keys():
            origin_cells[ent['current_cell_id']] = 1
        else:
            origin_cells[ent['current_cell_id']] += 1

    for ent in data:
        for pred in ent['predictions']:
            if (float(pred['probability']) > 0.5 and int(pred['cell_id']) != int(ent['current_cell_id'])):
                add_mobility_list(mobilities, origin_cells, pred['cell_id'], ent['current_cell_id'])

    for ele in mobilities:
        if ele['dest_cell'] in origin_cells.keys():
            ele['users'] = origin_cells[ele['dest_cell']]

    url = 'http://' +confs["fmc_ip"] +':5000/icnaas/api/v1.0/fmc_manager'
    #print url

    for mob in mobilities:
        #print {'mobility_prediction' : mob}
        r = requests.post(url, json={'mobility_prediction' : mob})
        #print r


confs = {}

if __name__ == '__main__':

    with open('/home/ubuntu/provisioning_MPM.conf', 'r') as opts:
        for line in opts.readlines():
            confs[line.split('=')[0]] = line.split('=')[1][:-1]

    #register_mobaas(10, confs['public_ip'], confs['mobaas_ip'])

    app.run(debug=False, host='0.0.0.0', port=6000)
