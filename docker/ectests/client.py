# Copyright 2020 The ChubaoFS Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

# -*- coding: utf-8 -*-
from loguru import logger
import os
import master
import const
import json
import utils
logger.add("/cfs/log/ec_test.log", rotation="500 MB")
logger.level("DEBUG")

class Client:
    def __init__(self, path):
        self.mount_point = path
        self.file_map = {}

    def start(self):
        master.admin_create_vol(name=const.testVol1, capacity=30, owner="ltptest", mp_count=3, replica_num=3, size=120, follower_read=True, cross_zone=False, zone_name="default", enable_token=False)
        if not os.path.exists(self.mount_point):
            os.mkdir(self.mount_point)
        j_file = '/cfs/conf/ec-client.json'
        client_conf = {}
        client_conf['masterAddr'] = const.masterHost
        client_conf['mountPoint'] = self.mount_point
        client_conf['volName'] = const.testVol1
        client_conf['owner'] = "ltptest"
        client_conf['logDir'] = "/cfs/log"
        client_conf['logLevel'] = "info"
        client_conf['consulAddr'] = "http://192.168.0.101:8500"
        client_conf['exporterPort'] = 9501
        client_conf['profPort'] = "17510"
        client_conf['authenticate'] = False
        client_conf['ticketHost'] = "192.168.0.14:8080,192.168.0.15:8081,192.168.0.16:8082"
        client_conf['enableHTTPS'] = False
        client_conf['accessKey'] = "39bEF4RrAQgMj6RV"
        client_conf['secretKey'] = "TRL6o3JL16YOqvZGIohBDFTHZDEcFsyd"

        with open(j_file, 'w') as f:
            json.dump(client_conf, f, sort_keys=False, indent=4, separators=(', ', ': '))

        os.system('/cfs/bin/cfs-client -c /cfs/conf/ec-client.json')
        logger.info("cfs client started")
        os.chdir(const.mountPoint)

    def create_file(self, file):
        utils.gen_size_file(file, 4*1024)
        md5 = utils.gen_md5(file)
        self.file_map[file] = md5
