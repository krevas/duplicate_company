from typing import List
from multiprocessing import Process, Manager

import numpy as np
import Levenshtein
from Levenshtein import ratio
from Levenshtein import jaro_winkler

from utils import *

logger = get_logger()


class FindDuplicate:
    def __init__(self):
        self.company_threshold = 0.75
        self.avg_threshold = 0.75

    def load(self, raw_data):
        self.raw_data = raw_data
        self.dup_index_map = dict()

    def set_threshold(
        self,
        company_threshold: float = 0.75,
        avg_threshold: float = 0.75,
    ):
        self.company_threshold = company_threshold
        self.avg_threshold = avg_threshold

    @staticmethod
    def string_distance(text_a: str, text_b: str):
        if not text_a or not text_b:
            return 0.0
        max_len = max(len(text_a), len(text_b))
        leven_val = (max_len - Levenshtein.distance(text_a, text_b)) / float(max_len)
        ratio_val = ratio(text_a, text_b)
        jaro_val = jaro_winkler(text_a, text_b)
        logger.debug(f"{leven_val:0.2f}\t{ratio_val:0.2f}\t{jaro_val:0.2f}")
        return average_3(leven_val, jaro_val, ratio_val)

    def _check_duplicate(self, idx, dup_index_map):
        company_distance = self.string_distance(
            self.raw_data[idx[0]]["company"], self.raw_data[idx[1]]["company"]
        )
        
        if company_distance >= self.company_threshold:
            address_distance = self.string_distance(
                self.raw_data[idx[0]]["address"], self.raw_data[idx[1]]["address"]
            )
            sim = weighted_average_2(company_distance, address_distance)
            sim = np.round(sim, 3)
            if sim >= self.avg_threshold:
                if idx[0] in dup_index_map:
                    tmp_dict = dup_index_map.get(idx[0])
                    tmp_dict.update({idx[1]: sim})
                    dup_index_map.update({idx[0]: tmp_dict})
                else:
                    dup_index_map.update({idx[0]: {idx[1]: sim}})

                if idx[1] in dup_index_map:
                    tmp_dict = dup_index_map.get(idx[1])
                    tmp_dict.update({idx[0]: sim})
                    dup_index_map.update({idx[1]: tmp_dict})
                else:
                    dup_index_map.update({idx[1]: {idx[0]: sim}})

        return dup_index_map

    def _update_dup_index_map(self, index_map_list: List):
        for index_map in index_map_list:
            self.dup_index_map = merge_dict(self.dup_index_map, index_map)

    def _run(self, idx_list: List, final_list):
        dup_index_map = dict()
        for idx in idx_list:
            dup_index_map = self._check_duplicate(idx, dup_index_map)
        if dup_index_map:
            final_list.append(dup_index_map)

    def run(self, idx_list: List, multi_num: int = 1):
        manager = Manager()
        final_list = manager.list()

        if multi_num == 1:
            self._run(idx_list, final_list)
        else:
            procs = []
            n = len(idx_list) // multi_num
            for sep_list in chunks(idx_list, n):
                proc = Process(target=self._run, args=(sep_list, final_list))
                procs.append(proc)
                proc.start()
            for proc in procs:
                proc.join()

        self._update_dup_index_map(final_list)

    def result(self):
        return self.dup_index_map
