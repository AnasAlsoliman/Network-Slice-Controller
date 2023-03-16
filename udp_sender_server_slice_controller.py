import socket
import time
import math
import json
from threading import Thread
from multiprocessing import Process, Manager, Value
import select
import queue
import csv
import os


def get_injection_file_node(node_id):
    injection_file = open("nodes_config_files/" + node_id + "_traffic_file.txt")
    while True:
        try:
            traffic_parameters = json.load(injection_file)
            break
        except json.decoder.JSONDecodeError:
            print("failed to open traffic file of node", node_id, "we will try again.")
    injection_file.close()
    return traffic_parameters


def get_json_file(json_path):
    while True:
        try:
            config_file = open(json_path)
            node_parameters = json.load(config_file)
            config_file.close()
            return node_parameters
        except json.decoder.JSONDecodeError:
            print("failed to open (", json_path, "). Will try again.")


# Get the file of an individual node (new format).
def get_json_file_node(node_id):
    config_file = open("nodes_config_files/" + node_id + "_config_file.txt")
    while True:
        try:
            node_parameters = json.load(config_file)
            break
        except json.decoder.JSONDecodeError:
            print("failed to open config file of node", node_id, "we will try again.")
    config_file.close()

    # traffic file has the latest traffic attributes while this config file don't.
    traffic_parameters = get_injection_file_node(node_id)

    # Update the traffic attribute from the traffic file.
    for key, val in traffic_parameters.items():
        node_parameters[key] = val

    return node_parameters


def get_bs_col_ip():
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["node_type"] == "bs":
            col_ip = node_dict["col_ip"]
            return col_ip


def get_client_lte_info(client_col_address):
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["col_ip"] == client_col_address:
            client_lte_ip = node_dict["lte_ip"]
            imsi = node_dict["ue_imsi"]
            return client_lte_ip, node_id, imsi


def get_injection_parameters(node_id):
    traffic_parameters = get_injection_file_node(node_id)
    packet_size = traffic_parameters["packet_size"]
    desired_bandwidth = traffic_parameters["desired_bandwidth"]
    return packet_size, desired_bandwidth


def create_packet(data, sequence_number):

    padding = "0" * (10 - len(sequence_number))  # sequence_number should be exactly 10 bytes.
    sequence_number = padding + sequence_number

    # print("new message_size", message_size)
    # print("new timestamp", timestamp)

    timestamp = time.time()
    timestamp = str(timestamp)
    padding = "0" * (20 - len(timestamp))  # timestamp should be exactly 20 bytes.
    timestamp = timestamp + padding

    message = bytes(timestamp + sequence_number + data, 'utf')
    return message


def start_receiving_measurements(measurement_socket, client_address):

    global measurements_queue
    measurements_queue = queue.Queue()
    counter = 0

    client_col_ip = client_address[0]
    client_lte_ip, client_node_id, client_imsi = get_client_lte_info(client_col_ip)

    client_dataset_thread = Thread(target=create_client_dataset, args=(client_col_ip,))
    client_dataset_thread.start()

    while True:
        try:
            message = b''
            while len(message) < 31:
                remaining_bytes = measurement_socket.recv(31 - len(message))
                if len(remaining_bytes) == 0:
                    print("client", client_node_id, "closed the measurement socket")
                    client_dict = process_dict[client_node_id]
                    client_dict["status"] = "down"
                    process_dict[client_node_id] = client_dict
                    measurements_queue.put("exit")
                    measurement_socket.close()
                    return
                message = message + remaining_bytes
            # print(int(message[4:]))
            # print("message:", message)

            # used as a 'connectivity check' when the client get stuck at queue.get().
            if message[:5].decode('utf') == "hello":
                continue

            packet_size = int(message[1:7])
            delay = float(message[8:24])
            lost_packets = int(message[25:])

            # counter = counter + 1
            # msg = [client_node_id, counter]

            measurements_queue.put([packet_size, delay, lost_packets, client_node_id])

            # print("packet_size:", int(packet_size), "delay:", float(delay), "lost_packets:", int(lost_packets))
            # print("lost_packets:", int(lost_packets))
            # print("----------------------")
        except KeyboardInterrupt:
            print("\nCtrl+C on measurement for node", client_node_id)
            measurements_queue.put("exit")
            measurement_socket.shutdown(2)
            measurement_socket.close()
            return


def create_client_dataset(client_col_ip):

    global measurements_queue
    client_lte_ip, client_node_id, client_imsi = get_client_lte_info(client_col_ip)

    # while True:
    #     time.sleep(0.25)
    #     report_list = []
    #     while True:
    #         try:
    #             report = measurements_queue.get(block=False)
    #             if report == "exit":
    #                 print("closing dataset thread for node", client_node_id)
    #                 return
    #             report_list.append(report)
    #         except queue.Empty:
    #             break
    #     for item in report_list:
    #         print("This is node", client_node_id, "received", item[1], "from node", item[0])
    #         assert client_node_id == item[0]
    #     print("--------------------------")

    config_files_path = "nodes_config_files/"

    ppr_dataset_name = "ue_" + client_node_id + ".csv"
    ppr_dataset_path = "/root/ue_datasets/"
    ppr_dataset_file = open(ppr_dataset_path + ppr_dataset_name, "a")
    ppr_dataset_writer = csv.writer(ppr_dataset_file)

    scope_dataset_name = client_imsi[2:] + "_metrics.csv"
    scope_dataset_path = "/root/radio_code/scope_config/metrics/csv/"
    scope_dataset_file = open(scope_dataset_path + scope_dataset_name)
    scope_dataset_reader = csv.reader(scope_dataset_file)

    counter = 0

    # If the file does not exist, create and add the header to the file.
    if os.path.getsize(ppr_dataset_path + ppr_dataset_name) == 0:
        # Adding scope headers first.
        scope_header = next(scope_dataset_reader)
        # print("scope_header", scope_header)
        ppr_header = []
        for field in scope_header:
            if not field:  # Some columns are empty
                continue
            else:
                ppr_header.append(field)

        # Adding our custom headers.
        ppr_header.append("avg_delay")
        ppr_header.append("min_delay")
        ppr_header.append("max_delay")
        ppr_header.append("all_delay")
        ppr_header.append("message_size")
        ppr_header.append("desired_bandwidth")
        ppr_header.append("packets_per_250ms")
        ppr_header.append("bytes_per_250ms")
        ppr_header.append("lost_packets_per_250ms")
        ppr_header.append("slice_id")
        ppr_header.append("slice_resources")
        ppr_header.append("requested_slice_resources")
        ppr_header.append("action")
        ppr_header.append("action_values")
        ppr_header.append("loss")
        ppr_header.append("reward_agent")
        ppr_header.append("loss_target")
        ppr_header.append("counter")
        ppr_dataset_writer.writerow(ppr_header)

    ppr_dataset_file.close()

    # To place the pointer at the end of scope dataset and sync with ppr dataset.
    while True:
        try:
            scope_entry = next(scope_dataset_reader)
        except StopIteration:
            # measurements_pipeline = []
            break

    # Start building the dataset.
    while True:
        try:
            # If the next line fails, the rest of the "try" block will be skipped.
            scope_entry = next(scope_dataset_reader)

            report_list = []
            # Get all items in measurements_queue.
            while True:
                try:
                    report = measurements_queue.get(block=False)
                    if report == "exit":
                        print("closing dataset thread for node", client_node_id)
                        scope_dataset_file.close()
                        return
                    report_list.append(report)
                except queue.Empty:
                    break

            pkt_size_list = []
            delay_list = []
            lost_pkts_list = []

            # Organize measurements into three lists.
            for item in report_list:
                pkt_size_list.append(item[0])
                delay_list.append(item[1])
                lost_pkts_list.append(item[2])
                assert client_node_id == item[3]

            # A single csv entry.
            ppr_entry = []

            # Removing empty fields.
            for entry in scope_entry:
                if not entry:
                    continue
                else:
                    ppr_entry.append(entry)

            # Read the latest DRL parameters.
            model_file = open("nodes_config_files/" + client_node_id + "_model_file.txt")
            while True:
                try:
                    model_parameters = json.load(model_file)
                    break
                except json.decoder.JSONDecodeError:
                    print("failed to open model file of node", client_node_id, "we will try again.")
            model_file.close()
            action = model_parameters["action"]
            action_values = model_parameters["action_values"]
            loss = model_parameters["loss"]
            reward = model_parameters["reward"]
            loss_target = model_parameters["loss_target"]

            # Read current config file of this node_id.
            node_parameters = get_json_file_node(client_node_id)

            # Update the latest PRB values reported by srsLTE.
            node_parameters["sum_requested_prbs"] = ppr_entry[25]
            node_parameters["sum_granted_prbs"] = ppr_entry[26]

            # Read latest slice info.
            slice_resources = node_parameters["my_slice_resources"]
            slice_id = node_parameters["my_slice_id"]
            requested_slice_resources = node_parameters["requested_slice_resources"]
            message_size = node_parameters["packet_size"]
            desired_bandwidth = node_parameters["desired_bandwidth"]

            # Update the new config parameters of this node_id.
            node_file_path = config_files_path + client_node_id + "_config_file.txt"
            while True:
                try:
                    config_file = open(node_file_path, "w")
                    json.dump(node_parameters, config_file, indent=4)
                    config_file.close()
                    break
                except TypeError:
                    print("Failed to write", node_file_path, ", will try again")

            if len(delay_list) == 0:
                avg_delay = 0
                min_delay = 0
                max_delay = 0
            else:
                avg_delay = sum(delay_list) / len(delay_list)
                min_delay = min(delay_list)
                max_delay = max(delay_list)

            # We need to keep track of the updated injection parameters.
            # packet_size, desired_bandwidth = get_injection_parameters(node_id)

            packets_per_250ms = len(pkt_size_list)
            bytes_per_250ms = sum(pkt_size_list)
            lost_packets_per_250ms = sum(lost_pkts_list)

            counter += 1

            ppr_entry.append(avg_delay)
            ppr_entry.append(min_delay)
            ppr_entry.append(max_delay)
            ppr_entry.append(delay_list)
            ppr_entry.append(message_size)
            ppr_entry.append(desired_bandwidth)
            ppr_entry.append(packets_per_250ms)
            ppr_entry.append(bytes_per_250ms)
            ppr_entry.append(lost_packets_per_250ms)
            ppr_entry.append(slice_id)
            ppr_entry.append(slice_resources)
            ppr_entry.append(requested_slice_resources)
            ppr_entry.append(action)
            ppr_entry.append(action_values)
            ppr_entry.append(loss)
            ppr_entry.append(reward)
            ppr_entry.append(loss_target)
            ppr_entry.append(counter)

            # print("reward", reward, "from node", client_node_id)

            # Adding a new entry to ppr_dataset.
            with open(ppr_dataset_path + ppr_dataset_name, "a") as ppr_dataset_file:
                ppr_dataset_writer = csv.writer(ppr_dataset_file)
                ppr_dataset_writer.writerow(ppr_entry)

        except StopIteration:
            time.sleep(0.01)
            # pass
        except KeyboardInterrupt:
            print("\nctrl+c from dataset thread")
            break

    print("closing dataset files")
    ppr_dataset_file.close()
    scope_dataset_file.close()


def start_traffic_injection(client_col_ip):

    client_lte_ip, client_node_id, client_imsi = get_client_lte_info(client_col_ip)
    injection_port = 9900 + int(client_imsi[13:])
    injection_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    message_size, desired_bandwidth = get_injection_parameters(client_node_id)

    num_of_packets = message_size / MTU  # How many packets we need to send a single message.
    num_of_MTU_packets = math.floor(num_of_packets) # Padding the last message to MTU size.

    sequence_number = 1
    total_sent = 0  # For print purposes only.
    sleep_time = message_size / desired_bandwidth  # inter-arrival time of messages (not packets).

    print('Message size: ', message_size / 1000, "KB")
    print('Desired bandwidth: ', desired_bandwidth * 8 / 1000000, "Mbps")
    print("Packet inter-arrivals: ", sleep_time, "second")

    current_time = time.time()

    while True:

        try:
            # Check for new injection parameters every 0.25 second (SCOPE maximum resolution).
            if time.time() - current_time > 0.25:
                client_dict = process_dict[client_node_id]
                # print(client_dict["status"])
                if client_dict["status"] == "down":
                    print("Signal from measurement process to close injection for node", client_node_id)
                    break
                current_time = time.time()
                total_sent = 0
                new_message_size, new_desired_bandwidth = get_injection_parameters(client_node_id)
                if message_size == new_message_size and desired_bandwidth == new_desired_bandwidth:
                    print("No updated parameters for node", client_node_id)
                else:
                    # print("new injection parameters for node", client_node_id, "- message_size", new_message_size, "desired_bandwidth", new_desired_bandwidth)
                    message_size = new_message_size
                    desired_bandwidth = new_desired_bandwidth
                    sleep_time = message_size / desired_bandwidth
                    num_of_packets = message_size / MTU
                    num_of_MTU_packets = math.floor(num_of_packets)

                    bw = '{0:.4f}'.format(desired_bandwidth * 8 / 1000000)
                    ia = '{0:.4f}'.format(sleep_time)

                    print("Parameters updated for node", client_node_id,
                          ', Bandwidth:', bw, "Mbps", ", inter-arrivals:", ia, "second")


            ############### Start Injection #################
            for i in range(0, num_of_MTU_packets):
                packet = create_packet(MTU_data, str(sequence_number))
                sent = injection_socket.sendto(packet, (client_lte_ip, injection_port))
                total_sent = total_sent + sent
                sequence_number = sequence_number + 1
                if sequence_number == 9999999999:
                    sequence_number = 1

            # Both actually works.
            # remaining_bytes = message_size - total_sent
            remaining_bytes = message_size - (MTU * num_of_MTU_packets)

            # print("total bytes sent so far:", total_sent)
            # print("total bytes sent so far:", num_of_MTU_packets * (len(MTU_data) + header_size))
            # print("last packet size:", remaining_bytes)
            # print("last packet size:", message_size - total_sent)

            if remaining_bytes != 0:  # We still have less-than-MTU bytes to send.
                if remaining_bytes <= header_size:  # Minimum packet size is header size.
                    packet = create_packet('', str(sequence_number))
                    sent = injection_socket.sendto(packet, (client_lte_ip, injection_port))
                else:
                    packet = create_packet(MTU_data[:remaining_bytes - header_size], str(sequence_number))
                    sent = injection_socket.sendto(packet, (client_lte_ip, injection_port))
                # print("last packet sent", sent)
                # print("last packet sent", len(packet))
                total_sent = total_sent + sent
                sequence_number = sequence_number + 1
                if sequence_number == 9999999999:
                    sequence_number = 1
            ################ End Injection #################

            # print("----------------------------")
            time.sleep(sleep_time)

        # This happens when trying to sendto() a non-empty buffer.
        # This will never happen if the socket is not set to non-blocking.
        except BlockingIOError:
            select.select([], [injection_socket], [])
            print("buffer is full (the remaining of the message will be lost)")
            time.sleep(sleep_time)

        except KeyboardInterrupt:
            print("\nctrl+C detected on injection process for node", client_node_id)
            break

    injection_socket.close()
    print("existing injection process for client", client_node_id)


def start_receiving_connections():

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    while True:
        try:
            server_socket.bind((col_ip, col_port))
            server_socket.listen(10)
            print("Starting a TCP server on", col_ip, "at port", col_port)
            break
        except OSError:
            print("Couldn't bind on IP", col_ip, "and port", col_port)

    while True:
        try:
            client_socket, client_address = server_socket.accept()
            print("UE", client_address[0], "connected from port", client_address[1], "on the Colosseum internal network")

            client_lte_ip, client_node_id, client_imsi = get_client_lte_info(client_address[0])
            client_dict = {}
            client_dict["status"] = "up"
            client_dict["socket"] = client_socket
            process_dict[client_node_id] = client_dict

            client_injection_process = Process(target=start_traffic_injection, args=(client_address[0],))
            client_injection_process.start()

            client_measurement_process = Process(target=start_receiving_measurements, args=(client_socket, client_address,))
            client_measurement_process.start()

        # This happens when closing a socket on accept().
        except ConnectionAbortedError:
            print("Server socket closed")
            server_socket.close()
            break

        except KeyboardInterrupt:
            print("\nCtrl+C: closing down entire server")
            for node_id, node_dict in process_dict.items():
                node_dict["status"] = "down"
                node_dict["socket"].close()
                # server_socket.shutdown(2)
            server_socket.close()
            break


if __name__ == "__main__":

    os.system("rm -rf ue_datasets")
    os.system("mkdir ue_datasets")

    lte_ip = "172.16.0.1"

    col_ip = get_bs_col_ip()
    col_port = 5555

    MTU = 1472  # 1500 - 20 (IP) - 8 (UDP)
    header_size = 30
    MTU_data = 'x' * (MTU - header_size)

    process_dict = Manager().dict()

    start_receiving_connections()

    # UDP_IP = "127.0.0.1"
    # UDP_PORT = 5005
    # sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # header_size = 30




