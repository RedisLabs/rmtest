# pylint: disable=missing-docstring, invalid-name, broad-except

import os
import time
import uuid
import logging as log
from . import DisposableRedis

class Cluster(object):

    def __init__(self, num_nodes=3, path='redis-server', **extra_args):

        self.common_conf = {
            'cluster-enabled': 'yes',
            'cluster-node-timeout': '5000',
        }
        self.common_conf.update(extra_args)
        self.num_nodes = num_nodes
        self.nodes = []
        self.ports = []
        self.confs = []
        self.redis_path = path
        self.extra_args = extra_args

    def _node_by_slot(self, slot):

        slots_per_node = int(16384 / len(self.ports)) + 1
        for i, node in enumerate(self.nodes):

            start_slot = i * slots_per_node
            end_slot = start_slot + slots_per_node - 1
            if end_slot > 16383:
                end_slot = 16383
            if start_slot <= slot <= end_slot:
                return node

        return None

    def _setup_cluster(self):

        for i, node in enumerate(self.nodes):
            conn = node.client()
            conn.cluster('RESET')

        slots_per_node = int(16384 / len(self.ports)) + 1
        for i, node in enumerate(self.nodes):
            assert isinstance(node, DisposableRedis)
            conn = node.client()
            for port in self.ports:
                conn.cluster('MEET', '127.0.0.1', port)

            start_slot = i * slots_per_node
            end_slot = start_slot + slots_per_node
            if end_slot > 16384:
                end_slot = 16384

            conn.cluster('ADDSLOTS', *(str(x) for x in range(start_slot, end_slot)))


    def _wait_cluster(self, timeout_sec):

        st = time.time()
        ok = 0

        while st + timeout_sec > time.time():
            ok = 0
            for node in self.nodes:
                status = node.client().cluster('INFO')
                if status.get('cluster_state') == 'ok':
                    ok += 1
            if ok == len(self.nodes):
                print("All nodes OK!")
                return

            time.sleep(0.1)
        raise RuntimeError("Cluster OK wait loop timed out after %s seconds" % timeout_sec)



    def _start_nodes(self):

        # Assign a random "session id"
        uid = uuid.uuid4().hex
        for i in range(self.num_nodes):

            conf = self.common_conf.copy()
            nodeconf = 'node-%s.%d.conf' % (uid, i)
            conf['cluster-config-file'] = nodeconf
            self.confs.append(nodeconf)


            node = DisposableRedis(path=self.redis_path, **conf)
            node.force_start()
            node.start()

            self.nodes.append(node)
            self.ports.append(node.port)


    def start(self):

        self._start_nodes()
        self._setup_cluster()

        self._wait_cluster(10)

        return self.ports


    def broadcast(self, *args):

        rs = []
        for node in self.nodes:

            conn = node.client()
            rs.append(conn.execute_command(*args))

        return rs


    def stop(self):

        for i, node in enumerate(self.nodes):
            assert isinstance(node, DisposableRedis)
            try:
                node.stop()
            except Exception as err:
                log.error("Error stopping node: %s", err)
            try:
                os.unlink(self.confs[i])
            except OSError:
                pass

    def client_for_key(self, key):

        conn = self.nodes[0].client()
        slot = conn.cluster('KEYSLOT', key)
        node = self._node_by_slot(slot)

        return node.client()
