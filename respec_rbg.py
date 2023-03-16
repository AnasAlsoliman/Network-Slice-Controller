
import scope_api as sc
import argparse
import json
import numpy as np


def print_masks():
    path = "/root/radio_code/scope_config/slicing/slice_allocation_mask_tenant_"
    # path = "/home/thunderspirits/Desktop/to_do/test_masks/slice_allocation_mask_tenant_"
    extension = ".txt"

    for i in range(0, 3):
        mask_file = open(path + str(i) + extension)
        mask = mask_file.read()
        mask = mask[:17]
        counter = 0
        for b in mask:
            if b == "1":
                counter += 1
        print("Slice", i, "mask:", mask, "with size:", counter)
        mask_file.close()


def get_json_file_node(node_id):
    node_id = str(node_id)
    config_file = open("nodes_config_files/" + node_id + "_config_file.txt")
    while True:
        try:
            node_parameters = json.load(config_file)
            break
        except json.decoder.JSONDecodeError:
            print("failed to open config file of node", node_id, "we will try again.")
    config_file.close()

    return node_parameters


def get_mask_size(slice_id):
    path = "/root/radio_code/scope_config/slicing/slice_allocation_mask_tenant_"
    extension = ".txt"
    mask_file = open(path + str(slice_id) + extension)
    mask = mask_file.read()
    # mask = mask[:17]
    counter = 0
    for b in mask:
        if b == "1":
            counter += 1

    mask_file.close()
    return mask, counter


def update_traffic_parameters(node_id, packet_size, desired_bandwidth, slice_prb):
    # User input bandwidth in Kbps.
    desired_bandwidth = desired_bandwidth * 1000 / 8

    node_parameters = get_json_file_node(node_id)

    # Read current available resources.
    slice_id = node_parameters["my_slice_id"]

    slice_prb = int(slice_prb)

    if slice_prb < 1:
        slice_prb = 1

    # Request the resources from SCOPE.
    if slice_prb == -1:
        print("skipping slice_prb")
    else:
        sc.write_slice_allocation_relative(slice_id, slice_prb)

    # Read how much we actually got.
    mask, prb = get_mask_size(slice_id)

    # r = np.random.uniform(low=0.0, high=1.0, size=None)
    # desired_bandwidth = desired_bandwidth * r

    # Update only 4 attributes.
    traffic_parameters = {}
    traffic_parameters["packet_size"] = packet_size
    traffic_parameters["desired_bandwidth"] = desired_bandwidth
    traffic_parameters["my_slice_resources"] = prb
    traffic_parameters["requested_slice_resources"] = slice_prb

    # Only write on your designated files.
    node_id = str(node_id)
    node_file_path = "nodes_config_files/" + node_id + "_traffic_file.txt"
    while True:
        try:
            config_file = open(node_file_path, "w")
            json.dump(traffic_parameters, config_file, indent=4)
            config_file.close()
            # print("traffic_parameters", traffic_parameters)
            break
        except TypeError:
            print("Failed to write", node_file_path, "will try again")
            print(traffic_parameters)
            # sys.exit("closing...")

    print("Node", node_id, "PRBs:", slice_prb)
    print("Slice", slice_id, "mask:", mask, "with size:", prb)
    print("Node", node_id, "packet size:", packet_size, "bytes")
    print("Node", node_id, "bandwidth:", desired_bandwidth, "Bps")
    print("Node", node_id, "bandwidth:", desired_bandwidth * 8 / 1000000, "Mbps")
    print("Therefore, expected packet inter-arrivals: ", packet_size / desired_bandwidth, "second")
    print_masks()
    # print("----------------------------------")


parser = argparse.ArgumentParser()
parser.add_argument('--slice_rbg', type=int, nargs='+')
parser.add_argument('--slice_sched', type=int, nargs='+')
parser.add_argument('--glob_sched', type=int, nargs='+')
parser.add_argument('--slice_mask', type=str, nargs='+')
parser.add_argument('--set_traffic', type=int, nargs='+')

args = parser.parse_args()

if args.slice_rbg:
    slice_idx = args.slice_rbg[0]
    slice_rbgs = args.slice_rbg[1]
    print("Slice ID:", args.slice_rbg[0])
    print("Requested RBG:", args.slice_rbg[1])
    sc.write_slice_allocation_relative(slice_idx, slice_rbgs)
    print_masks()


if args.slice_mask:
    slice_idx = args.slice_mask[0]
    slice_mask2 = args.slice_mask[1]

    # print("slice_mask2", slice_mask2)

    sc.set_slice_resources(int(slice_idx), slice_mask2)

    # path = "/root/radio_code/scope_config/slicing/slice_allocation_mask_tenant_" + slice_idx + ".txt"
    # path = "/home/thunderspirits/Desktop/to_do/test_masks/slice_allocation_mask_tenant_" + str(slice_idx) + ".txt"

    # mask_file = open(path, "w")
    # mask_file.write(str(slice_mask2))
    # mask_file.close()

    print_masks()


if args.slice_sched:
    slice_idx = args.slice_sched[0]
    if args.slice_sched[1] == 0:
        sch = sc.SchedPolicy.round_robin
    if args.slice_sched[1] == 1:
        sch = sc.SchedPolicy.waterfilling
    if args.slice_sched[1] == 2:
        sch = sc.SchedPolicy.proportionally

    print('slice_idx:', slice_idx)
    print('scheduling_policy', sch)
    print('scheduling_policy value:', sch.value)

    sc.set_slice_scheduling(slice_idx, sch)

if args.glob_sched:
    g_sc = args.glob_sched[0]
    print("global scheduling", g_sc)
    sc.set_scheduling(g_sc)

if args.set_traffic:
    node_id = args.set_traffic[0]
    packet_size = args.set_traffic[1]
    desired_bandwidth = args.set_traffic[2]
    slice_prb = args.set_traffic[3]

    update_traffic_parameters(node_id, packet_size, desired_bandwidth, slice_prb)

