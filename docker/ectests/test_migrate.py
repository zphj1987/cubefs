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
import utils
import const
import json
import client
import time
logger.add("/cfs/log/ec_test.log", rotation="500 MB")
logger.level("DEBUG")

class TestMigrate:

    def test_migrate(self):
        cfs_client = client.Client(const.mountPoint)
        cfs_client.start()
        for i in range(1, 1000):
            cfs_client.create_file(str(i))
        time.sleep(5)

        is_migrate_complete = master.check_migrate_completion(const.mountPoint)

        if is_migrate_complete:
            for i in range(1, 1000):
                md5 = utils.gen_md5(str(i))
                assert md5 == cfs_client.file_map[str(i)]
        os.system('umount '+const.mountPoint)
