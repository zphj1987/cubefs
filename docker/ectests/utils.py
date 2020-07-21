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
import paramiko
import const as const
import hashlib
import random
import os
__author__ = 'xuxihao1'
__date__ = '@date'
__description__ = """ ssh login """


class SSHUtils(object):
    def __init__(self, host):
        self.host = host
        self.port = const.sshPort
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    def ssh_exec_command(self, command):
        try:
            self.ssh_client.connect(self.host, self.port, const.user, const.password)
            std_in, std_out, std_err = self.ssh_client.exec_command(command)
            for line in std_out:
                print(line.strip("\n"))
        except Exception as e:
            print("error: {}", e)
        finally:
            self.ssh_client.close()


def gen_md5(name):
    md5 = None
    file_name = "file_" + name + ".txt"
    if os.path.isfile(file_name):
        f = open(file_name, 'rb')
        md5_obj = hashlib.md5()
        md5_obj.update(f.read())
        hash_code = md5_obj.hexdigest()
        f.close()
        md5 = str(hash_code).lower()
    return md5


def gen_size_file(name, size):
    filePath = "file_" + name + ".txt"
    ds = 0
    with open(filePath, "w", encoding="utf8") as f:
        while ds < size:
            f.write(str(round(random.uniform(-1000, 1000), 2)))
            f.write("\n")
            ds = os.path.getsize(filePath)
