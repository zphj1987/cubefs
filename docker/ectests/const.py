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
"""
ssh login
"""
sshPort = 22
timeout = 30
user = "root"
password = "*******"

masterHost = "192.168.0.11:17010"
"""
test volume
"""
testVol1 = "testVol1"
testVol2 = "testVol2"
testVol3 = "testVol3"
mountPoint = "/cfs/ec-mnt"

"""
master param
"""
addrKey = "addr"
diskPathKey = "disk"
nameKey = "name"
idKey = "id"
countKey = "count"
startKey = "start"
enableKey = "enable"
thresholdKey = "threshold"
dataPartitionSizeKey = "size"
metaPartitionCountKey = "mpCount"
volCapacityKey = "capacity"
volOwnerKey = "owner"
volAuthKey = "authKey"
replicaNumKey = "replicaNum"
followerReadKey = "followerRead"
authenticateKey = "authenticate"
akKey = "ak"
keywordsKey = "keywords"
zoneNameKey = "zoneName"
crossZoneKey = "crossZone"
tokenKey = "token"
tokenTypeKey = "tokenType"
enableTokenKey = "enableToken"
userKey = "user"
typeKey = "type"
userPwdKey = "pwd"
userIdKey = "user_id"
accessKeyKey = "access_key"
secretKeyKey = "secret_key"
volumeKey = "volume"
policyKey = "policy"


"""
metanode param
"""
pidKey = "pid"
inodeKey = "ino"
parentInodeKey = "parentIno"


"""
datanode param
"""
partitionIDKey = "partitionID"
extentIDKey = "extentID"
raftIDKey = "raftID"
