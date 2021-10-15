import time
import argparse

from utils import *
from core.duplicate import FindDuplicate


def load_alphabet(path: str, size: int):
    alpha_set = dict()
    cnt = 0
    for idx, line in enumerate(open(path)):
        if idx == 0:
            continue
        line = line.strip()
        if line:
            split = line.split(",")
            if len(split) < 7:continue
            company = text_clean(split[1])
            if company:
                if company[0:2] in alpha_set:
                    tmp = alpha_set.get(company[0:2])
                    tmp.add(cnt)
                    alpha_set.update({company[0:2]: tmp})
                else:
                    tmp = set()
                    tmp.add(cnt)
                    alpha_set.update({company[0:2]: tmp})
                cnt += 1
                if cnt == size:
                    break

    return dict(sorted(alpha_set.items()))

def load_corpus(path: str):
    raw_data = []
    for idx, line in enumerate(open(path)):
        if idx == 0:
            continue
        line = line.strip()
        if line:
            split = line.split(",")
            if len(split) < 7:continue
            company = text_clean(split[1])
            if company:
                raw_data.append(
                    {
                        "company": company,
                        "address": text_clean(split[6]),
                        "origin_company": split[1].replace('"', ""),
                        "origin_address": split[6].replace('"', ""),
                    }
                )
    return raw_data


def main(args):
    idx_info = load_alphabet(args.path, args.data_size)

    writer = open(args.output, "w")
    writer.write(f"company_a\taddress_a\tcompany_b\taddress_b\tsimilarity\n")
    start_time = time.time()

    fd = FindDuplicate()

    raw_data = load_corpus(args.path)
    fd.load(raw_data)

    output = []

    for alphabet, idx_list in idx_info.items():
        idx_list = list(idx_list)
        logger.info(f"{alphabet} : {len(idx_list):,d}")
        if len(idx_list) == 1:
            continue
        if len(idx_list) < 3000:
            if len(output) > 100000000:
                for i in range(0, len(idx_list) - 1):
                    for j in range(i + 1, len(idx_list)):
                        output.append((idx_list[i], idx_list[j]))
                        if len(output) > 100000000:
                            fd.run(output, multi_num=args.multi_num)
                            output = []
                if output:
                    fd.run(output, multi_num=args.multi_num)
                    output = []
            else:
                for i in range(0, len(idx_list) - 1):
                    for j in range(i + 1, len(idx_list)):
                        output.append((idx_list[i], idx_list[j]))
        else:
            for i in range(0, len(idx_list) - 1):
                for j in range(i + 1, len(idx_list)):
                    output.append((idx_list[i], idx_list[j]))
                    if len(output) > 100000000:
                        fd.run(output, multi_num=args.multi_num)
                        output = []
            if output:
                fd.run(output, multi_num=args.multi_num)
                output = []
            
    dup_index_map = fd.result()

    for k, v in sorted(dup_index_map.items()):
        for idx, sim in sorted(v.items(), key=lambda item: item[1], reverse=True):
            if int(sim) == 1:continue
            writer.write(
                f"{raw_data[k]['origin_company']}\t{raw_data[k]['origin_address']}\t{raw_data[idx]['origin_company']}\t{raw_data[idx]['origin_address']}\t{sim:0.2f}\n"
            )

    logger.info(f"Execution time : {(time.time() - start_time):0.2f} seconds")

    writer.close()


if __name__ == "__main__":
    logger = get_logger()

    cli_parser = argparse.ArgumentParser()

    cli_parser.add_argument("--path", default="data/companies_sorted.csv", type=str)
    cli_parser.add_argument("--output", default="./result.tsv", type=str)
    cli_parser.add_argument("--multi_num", default=8, type=int)
    cli_parser.add_argument("--data_size", default=500000, type=int)

    cli_args = cli_parser.parse_args()

    main(cli_args)
