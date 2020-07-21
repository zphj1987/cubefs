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
import const
import requests
import json


def admin_create_vol(name, capacity, owner, mp_count, replica_num, size, follower_read, cross_zone,
                     zone_name, enable_token):
    url = "http://" + const.masterHost + "/admin/createVol"
    parm = {const.nameKey: name, const.volCapacityKey: capacity, const.volOwnerKey: owner,
            const.metaPartitionCountKey: mp_count,
            const.replicaNumKey: replica_num, const.dataPartitionSizeKey: size,
            const.followerReadKey: follower_read,
            const.crossZoneKey: cross_zone, const.zoneNameKey: zone_name, const.enableTokenKey: enable_token}
    response = requests.get(url, params=parm)
    assert response.status_code == 200
    assert json.loads(response.text)['code'] == 0
    return response.text


def check_migrate_completion(volume):
    url = "http://" + const.masterHost + "/admin/checkMigrate"
    parm = {const.nameKey: volume}
    return True
