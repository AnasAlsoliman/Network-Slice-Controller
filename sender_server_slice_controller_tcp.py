
'''
Version 8
This version is the same as v7 but with all the fixes and works on SCOPE.
'''


import socket
from threading import Thread
from multiprocessing import Process, Manager, Value
import select
import time
import json
import csv
import os
from queue import Queue
from signal import signal, SIGINT
from sys import exit


# Get the big file that contains all nodes.
# Old function but other functions still use this (need to change this later).
def get_json_file(json_path):
    while True:
        try:
            config_file = open(json_path)
            node_parameters = json.load(config_file)
            config_file.close()
            return node_parameters
        except json.decoder.JSONDecodeError:
            print("failed to open", json_path, ". Will try again.")


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

    # while True:
    #     try:
    #         config_file = open(node_file_path)
    #         node_parameters = json.load(config_file)
    #         config_file.close()
    #         return node_parameters
    #     except json.decoder.JSONDecodeError:
    #         print("failed to open", node_file_path, ". we will use the backup file.")
    #         config_file = open("nodes_config_files/" + node_id + "_config_file_backup.txt")
    #         node_parameters = json.load(config_file)
    #         config_file.close()
    #
    #         # Fixing the corrupted file.
    #         config_file = open(node_file_path, "w")
    #         json.dump(node_parameters, config_file, indent=4)
    #         config_file.close()
    #         return node_parameters


# Get the traffic file of an individual node.
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


def get_my_col_ip():
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["this_is_my_node"]:
            col_ip = node_dict["col_ip"]
            return col_ip


def get_client_dict(client_lte_address):
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["lte_ip"] == client_lte_address:
            col_ip = node_dict["col_ip"]
            imsi = node_dict["ue_imsi"]
            return col_ip, node_id, imsi


# For those functions who are too lazy to extract the individual attributes.
def get_injection_parameters(node_id):
    # config_files_path = "nodes_config_files/"
    # node_file_path = config_files_path + node_id + "_config_file.txt"
    # with open(node_file_path) as config_file:
    #     node_parameters = json.load(config_file)

    # config_file = open(node_file_path)
    # node_parameters = json.load(config_file)
    # config_file.close()

    traffic_parameters = get_injection_file_node(node_id)
    packet_size = traffic_parameters["packet_size"]
    desired_bandwidth = traffic_parameters["desired_bandwidth"]
    return packet_size, desired_bandwidth

    # json_file = open("nodes_parameters_file.txt")
    # nodes_parameters = json.load(json_file)
    # json_file.close()
    # packet_size = nodes_parameters[node_id]["packet_size"]
    # desired_bandwidth = nodes_parameters[node_id]["desired_bandwidth"]
    # return packet_size, desired_bandwidth


def get_slice_parameters(node_id):

    # config_files_path = "nodes_config_files/"
    # node_file_path = config_files_path + node_id + "_config_file.txt"

    # config_file = open(node_file_path)
    # node_parameters = json.load(config_file)
    # config_file.close()

    node_parameters = get_json_file_node(node_id)
    slice_resources = node_parameters["my_slice_resources"]
    slice_id = node_parameters["my_slice_id"]
    return slice_id, slice_resources

    # json_file = open("nodes_parameters_file.txt")
    # nodes_parameters = json.load(json_file)
    # json_file.close()
    # slice_id = nodes_parameters[node_id]["my_slice_id"]
    # slice_resources = nodes_parameters[node_id]["my_slice_resources"]
    # return slice_id, slice_resources


# Main function to start the server.
def receiving_clients():
    serverIP = '172.16.0.1'
    # serverIP = 'srs_spgw_sgi'
    # serverIP = 'localhost'
    serverPort = 9999
    col_ip = get_my_col_ip()



    # Used to close all clients connections from the server (sender) side.
    kill_switch = Value('i')
    kill_switch.value = False
    global measurements_pipelines
    clients_list = []
    disconnections = {}

    ######### Create Traffic Injection TCP Socket ##########
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    while True:
        try:

            server_socket.bind((serverIP, serverPort))
            server_socket.listen(10)
            print("Starting a TCP server on", serverIP, "at port", serverPort)
            break
        except OSError:
            print("BS LTE Interface", serverIP, "is not ready yet")

    while True:
        try:
            client_socket, client_address = server_socket.accept()

            # Keeps track of how many disconnections we got for each client.
            try:
                disconnections[client_address[0]] = disconnections[client_address[0]] + 1
            except KeyError:
                disconnections[client_address[0]] = 1

            connection_number = disconnections[client_address[0]]
            print("UE", client_address[0], "connected from port", client_address[1], "- connection number:", connection_number)

            client_col_ip, client_node_id, client_imsi = get_client_dict(client_address[0])

            ######### Create Measurements Receiving TCP Socket ##########
            measurement_port = 9900 + int(client_imsi[13:])
            measurements_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            measurements_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            print("col_ip", col_ip)
            measurements_socket.bind((col_ip, measurement_port))
            measurements_socket.listen(10)
            print("Creating a measurements_socket server on", col_ip, "for client node", client_node_id,
                  "at port", measurement_port)

            client_data = [client_socket, client_address]

            client_thread = Process(target=start_speedtest, args=(client_data,))
            client_thread.start()

            client_measurements_thread = Process(target=recv_client_measurements, args=(client_address,measurements_socket,))
            client_measurements_thread.start()

        # This happens when closing a socket on accept().
        except ConnectionAbortedError:
            print("Server socket closed")
            server_socket.close()
            kill_switch.value = True
            break

        except KeyboardInterrupt:
            print("\nCtrl+C detected")
            # server_socket.shutdown(2)
            server_socket.close()
            kill_switch.value = True
            break

        # except OSError:
        #     print("We will try again shortly. col_ip:", col_ip)
        #     time.sleep(2)


def create_message(data, message_size):

    timestamp = time.time()
    timestamp = str(timestamp)
    padding = "0" * (20 - len(timestamp))  # timestamp should be exactly 20 bytes.
    timestamp = timestamp + padding

    padding = "0" * (7 - len(message_size))  # message_size should be exactly 7 bytes.
    message_size = padding + message_size

    # print("new message_size", message_size)
    # print("new timestamp", timestamp)

    message = bytes(timestamp + message_size + data, 'utf')
    return message


def start_speedtest(client_data):
    # global kill_switch
    client_socket = client_data[0]
    client_address = client_data[1]

    client_lte_address = client_address[0]
    col_ip, node_id, imsi = get_client_dict(client_lte_address)
    message_size, desired_bandwidth = get_injection_parameters(node_id)

    # client_socket.setblocking(False)

    # In MBytes.
    # desired_bandwidth = 500000  # *8 = 2Mbit/s
    # message_size = 5000  # *8 = 400,000 bit

    # 27-bytes reserved for the header, 20 for timestamp and 7 size field.
    data = 'x' * (message_size - 20 - 7)  # ex: 1-bytes * 1,000,000 message_size = 1MB = 8Mbits
    message = create_message(data, str(message_size))
    sleep_time = len(message) / desired_bandwidth

    print('Packet size: ', len(message) / 1000, "KB")
    print('Desired bandwidth: ', desired_bandwidth * 8 / 1000000, "Mbps")
    print("Packet inter-arrivals: ", sleep_time, "second")

    # data = data.encode()
    total_sent_one_second = 0
    total_sent_entire_session = 0
    rounds = 0
    max_inter_arrival = 1

    # It should be 1 instead of 0 but the last message created will not be sent and it will offset this initial 0.
    total_messages = 0

    start_timer = time.time()
    current_time = time.time()
    # modifier = 0

    full_throttle = False

    while True:

        # if kill_switch:
        #     print("Ending client thread at", client_address[0], "port", client_address[1])
        #     client_socket.shutdown(2)
        #     # The close() function happens outside the 'while' loop.
        #     break

        # What is this?
        # offset_timer = time.time()

        # Check if desired_bandwidth is met every second.
        if time.time() - current_time > 0.25:

            # 5% bandwidth difference
            bandwidth_offset = total_sent_one_second / desired_bandwidth  # 0.65

            # Main print.
            # print("Node", node_id, "received", total_sent_one_second * 8 / 1000000, "Mbits sent within", time.time() - current_time, "seconds")
            # print("actual bandwidth %", bandwidth_offset)

            # # Update sleep_time if there is 5% bandwidth difference between actual bandwidth and desired_bandwidth.
            # # In other words, we can tolerate up to 5% bandwidth difference.
            # if bandwidth_offset < 0.95 or bandwidth_offset > 1.05:
            #     timer_offset = 1 - bandwidth_offset  # 0.35
            #     sleep_time = sleep_time - (sleep_time * timer_offset)
            #
            #     print("timer_offset", timer_offset)
            #     print("new sleep_time", sleep_time)
            #     print("---------------------")

            total_sent_one_second = 0
            current_time = time.time()

#################################################################
            # Check for updated injection parameters every second
            new_message_size, new_desired_bandwidth = get_injection_parameters(node_id)
            if message_size == new_message_size and desired_bandwidth == new_desired_bandwidth:
                print("No updated parameters for node", node_id)
            else:
                message_size = new_message_size
                desired_bandwidth = new_desired_bandwidth
                data = 'x' * (message_size - 20 - 7)  # ex: 1-bytes * 1,000,000 message_size = 1MB = 8Mbits
                message = create_message(data, str(message_size))
                sleep_time = len(message) / desired_bandwidth
                if sleep_time > max_inter_arrival:
                    print("WARNING: sleep_time for node_id", node_id, "=", sleep_time, "seconds.")
                    print("message_size =", message_size, "bytes.")
                    print("desired_bandwidth =", desired_bandwidth, "bytes/second -", desired_bandwidth * 8 / 1000000, "Mbps")
                    print("forcing sleep_time to", max_inter_arrival, "seconds.")
                    print("-----------------------------------------")
                    sleep_time = max_inter_arrival
                # Main print. (UPDATE: print should change for correctness after using max_inter_arrival)
                bw = '{0:.4f}'.format(desired_bandwidth * 8 / 1000000)
                ia = '{0:.4f}'.format(sleep_time)
                # print("Injection parameters updated for node", node_id, ", Packet size:", len(message) / 1000, "KB",
                #       ', Desired bandwidth: ', bw, "Mbps", ", Packet inter-arrivals: ",
                #       ia, "second")
                print("Parameters updated for node", node_id,
                      ', Bandwidth:', bw, "Mbps", ", inter-arrivals:",
                      ia, "second")
#################################################################

        # # If you want to automatically stop after 5 seconds.
        # if current_time - start_timer > 5:
        #     break

        try:
            # send() function returns the number of bytes that was successfully pushed into the buffer.
            sent = client_socket.send(message)
            total_sent_one_second += sent
            total_sent_entire_session += sent
            # print ("we just sent", sent, "bytes. Total bytes that has been already sent:", total_sent_entire_session)

            # Sleep a little bit to throttle the speed at the desired bandwidth.
            # Due to the code-execution delay, we need to adjust the sleep time accordingly using the "modifier".
            # The placement of this line is important (must not be between "create_message" and "send").
            if not full_throttle:
                time.sleep(sleep_time)

            # Discarding the message portion that was already pushed into the buffer and only keeping the unsent part.
            message = message[sent:]

            # Creating a new message when we are done sending the current message.
            if len(message) == 0:
                message = create_message(data, str(message_size))
                total_messages += 1

            rounds += 1

        # This happens when trying to send() on a non-empty buffer.
        # This will never happen if the socket is not set to non-blocking.
        except BlockingIOError:
            # pass
            # print ('Blocking with', len(message), 'remaining')

            # here, 'select' will block (pause the program) until the buffer becomes empty.
            select.select([], [client_socket], [])

        # On localhost, this exception is returned when trying to send() to a closed socket.
        except ConnectionResetError:
            print("client at", client_address[0], "port", client_address[1], "closed the connection on localhost")
            break

        # When implemented over the network, this exception is returned when trying to send() to a closed socket.
        except BrokenPipeError:
            print("client at", client_address[0], "port", client_address[1], "closed the connection over the network")
            break

        except KeyboardInterrupt:
            print("\nctrl+C detected on send()")
            break

    # print("finished speedtest.....")
    # print("bandwidth consumed =", total_sent_entire_session, "bytes")
    # print("bandwidth consumed =", total_sent_entire_session / 1000000, "MB")
    print("bandwidth consumed =", total_sent_entire_session / 1000000000, "GB")
    print("total messages =", total_messages)
    print("############### End of Client Connection ################")
    # print("total rounds = " + str(rounds))
    client_socket.close()
    # client_data[2].close()  # Warning: recv_client_measurements() also closes this socket.


# This function is a separate python Process and it logs the message delays reported by the client.
# Each client will have its own dedicated measurement server and it will use col_ip (out-of-bound).
# This function is a dedicated python Process and it will spawn a separate create_client_dataset() thread.
# The logs will be used by create_client_dataset().
# IMPORTANT: both recv_client_measurements() and create_client_dataset() MUST set measurements_pipeline
# to 'global' to enable a shared access to the variable.
# This variable won't be seen by the same function spawned by another python Process.
# To be precise, the variable won't be seen by anyone from putside the process.
def recv_client_measurements(client_lte_address, measurements_socket):

    # Used to close all clients connections from the server (sender) side.
    global kill_switch
    kill_switch = False

    global measurements_pipeline

    # In this thread, the client will connect to this server using its client_col_address.
    # We logged its client_lte_address just in case we need it in later developments.
    # client_lte_address = client_lte_address[0]

    try:
        client_m_socket, client_m_address = measurements_socket.accept()
        print("UE", client_m_address[0], "connected from port", client_m_address[1], "for measurements")

        client_col_address = client_m_address[0]

        measurements_pipeline = []

        client_dataset_thread = Thread(target=create_client_dataset, args=(client_lte_address,))
        client_dataset_thread.start()

    # This happens when closing a socket on accept().
    except ConnectionAbortedError:
        print("Measurements socket closed")
        measurements_socket.close()
        kill_switch = True
        return

    except KeyboardInterrupt:
        print("\nClient", client_lte_address[0], "did not connect to its measurement socket and the server now is closing")
        measurements_socket.close()
        kill_switch = True
        return

    while True:
        try:
            message = client_m_socket.recv(16)
            if len(message) == 0:
                print("Client", client_col_address, "closed the measurements connection")
                break

            # Sometimes recv returns less than 8 bytes
            break_signal = False
            while len(message) < 16:
                remaining_bytes = client_m_socket.recv(16 - len(message))
                if len(remaining_bytes) == 0:
                    break_signal = True
                    break
                message = message + remaining_bytes

            # if len(message) != 16:
            #     print("delay size", len(message))
            if break_signal:
                print("Client", client_col_address, "closed the measurements connection")
                break
            message = float(message)
            # print("received delay =", message, "ms")
            # measurements_pipelines[client_col_address].append(message)
            measurements_pipeline.append(message)
            # print("pipeline size", len(measurements_pipeline), "with", measurements_pipeline)

        except KeyboardInterrupt:
            print("\nCtrl+C detected on receiving measurements")
            break
        if kill_switch:
            print("Server is closing down")
            break

    measurements_socket.shutdown(2)
    measurements_socket.close()
    client_m_socket.close()
    measurements_pipeline = "Exit"
    kill_switch = True

    # Without join(), the thread will not close properly and the datasets will be lost.
    client_dataset_thread.join()


def create_client_dataset(client_lte_address):

    global measurements_pipeline
    global kill_switch
    # client_address = client_data[1]
    # client_address = client_address[0] + str(client_address[1])  # Must be changed on scope.
    client_lte_address = client_lte_address[0]
    client_col_address, node_id, imsi = get_client_dict(client_lte_address)

    counter = 0

    # packet_size, desired_bandwidth = get_injection_parameters(node_id)
    # slice_id, slice_resources = get_slice_parameters(node_id)

    config_files_path = "nodes_config_files/"

    ppr_dataset_name = "ue_" + node_id + ".csv"
    ppr_dataset_path = "/root/ue_datasets/"
    ppr_dataset_file = open(ppr_dataset_path + ppr_dataset_name, "a")
    ppr_dataset_writer = csv.writer(ppr_dataset_file)

    scope_dataset_name = imsi[2:] + "_metrics.csv"
    scope_dataset_path = "/root/radio_code/scope_config/metrics/csv/"
    scope_dataset_file = open(scope_dataset_path + scope_dataset_name)
    scope_dataset_reader = csv.reader(scope_dataset_file)

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
        ppr_header.append("packet_size")
        ppr_header.append("desired_bandwidth")
        ppr_header.append("packets_per_250ms")
        ppr_header.append("bytes_per_250ms")
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

    # Accumulate initial measurements (not sure if necessary).
    # time.sleep(0.25)

    # To place the pointer at the end of scope dataset and sync with ppr dataset.
    while True:
        try:
            scope_entry = next(scope_dataset_reader)
        except StopIteration:
            measurements_pipeline = []
            break

    while True:

        if kill_switch:
            print("Exiting dataset thread via kill_switch")
            break
        if measurements_pipeline == "Exit":
            print("Exiting dataset thread via pipeline exit for node", node_id)
            break

        try:
            # If the next line fails, the rest of the "try" block will be skipped.
            scope_entry = next(scope_dataset_reader)

            # Because measurements_pipeline might get filled while processing its content.
            delay_list = measurements_pipeline.copy()

            # Emptying the pipeline because we consumed all current delay measurements.
            measurements_pipeline = []

            ppr_entry = []

            # Removing empty fields.
            for entry in scope_entry:
                if not entry:
                    continue
                else:
                    ppr_entry.append(entry)

            # Read the latest DRL parameters.
            model_file = open("nodes_config_files/" + node_id + "_model_file.txt")
            while True:
                try:
                    model_parameters = json.load(model_file)
                    break
                except json.decoder.JSONDecodeError:
                    print("failed to open model file of node", node_id, "we will try again.")
            model_file.close()
            action = model_parameters["action"]
            action_values = model_parameters["action_values"]
            loss = model_parameters["loss"]
            reward = model_parameters["reward"]
            loss_target = model_parameters["loss_target"]

            # if len(action_values) != 0:
            #     print(len(action_values))
            #     print(action_values)
            #     print(type(action_values))
            #     print(type(action_values[0]))
            #     print("--------------")

            # Read current config file of this node_id.
            node_parameters = get_json_file_node(node_id)

            # Update the latest PRB values reported by srsLTE.
            node_parameters["sum_requested_prbs"] = ppr_entry[25]
            node_parameters["sum_granted_prbs"] = ppr_entry[26]

            # Read latest slice info.
            slice_resources = node_parameters["my_slice_resources"]
            slice_id = node_parameters["my_slice_id"]
            requested_slice_resources = node_parameters["requested_slice_resources"]
            packet_size = node_parameters["packet_size"]
            desired_bandwidth = node_parameters["desired_bandwidth"]

            # Update the new config parameters of this node_id.
            node_file_path = config_files_path + node_id + "_config_file.txt"
            while True:
                try:
                    config_file = open(node_file_path, "w")
                    json.dump(node_parameters, config_file, indent=4)
                    config_file.close()
                    break
                except TypeError:
                    print("Failed to write", node_file_path, "will try again")



            # # Update backup files.
            # node_file_path = config_files_path + node_id + "_config_file_backup.txt"
            # config_file = open(node_file_path, "w")
            # json.dump(node_parameters, config_file, indent=4)
            # config_file.close()

            # print("node_id", node_id, "sum_requested_prbs", ppr_entry[25])
            # print("node_id", node_id, "sum_granted_prbs", ppr_entry[26])

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

            packets_per_250ms = len(delay_list)
            bytes_per_250ms = packet_size * packets_per_250ms

            counter += 1

            ppr_entry.append(avg_delay)
            ppr_entry.append(min_delay)
            ppr_entry.append(max_delay)
            ppr_entry.append(delay_list)
            ppr_entry.append(packet_size)
            ppr_entry.append(desired_bandwidth)
            ppr_entry.append(packets_per_250ms)
            ppr_entry.append(bytes_per_250ms)
            ppr_entry.append(slice_id)
            ppr_entry.append(slice_resources)
            ppr_entry.append(requested_slice_resources)
            ppr_entry.append(action)
            ppr_entry.append(action_values)
            ppr_entry.append(loss)
            ppr_entry.append(reward)
            ppr_entry.append(loss_target)
            ppr_entry.append(counter)

            # print("reward", reward, "from node", node_id)

            # Adding a new entry to ppr_dataset.
            with open(ppr_dataset_path + ppr_dataset_name, "a") as ppr_dataset_file:
                ppr_dataset_writer = csv.writer(ppr_dataset_file)
                ppr_dataset_writer.writerow(ppr_entry)

            # scope_timestamp = ppr_entry[0]
            # ppr_timestamp = ppr_entry[-1]
            # scope_timestamp = round(float(scope_timestamp))
            # ppr_timestamp = round(float(ppr_timestamp)*1000)
            # time_difference = ppr_timestamp - scope_timestamp
            # print("New entry added with time difference", time_difference, "ms")

            # # Same reason as measurements_pipelines.
            # delay_list = []

        # No more new scope entries. The scope dataset gets a new entry every 250ms.
        except StopIteration:
            time.sleep(0.01)
            # pass
            # print("waiting for a new scope dataset entry")
            # time.sleep(0.001)

        # client_col_address is removed by recv_client_measurements due to socket closure.
        # Update: this catch is no longer needed (will remove later).
        except KeyError:
            print("Exiting dataset thread for UE", node_id)
            ppr_dataset_file.close()
            scope_dataset_file.close()
            break

    # Datasets will be lost if not closed.
    print("closing dataset files")
    ppr_dataset_file.close()
    scope_dataset_file.close()

if __name__ == "__main__":
    os.system("rm -rf ue_datasets")
    os.system("mkdir ue_datasets")
    # time.sleep(10)
    # kill_switch = False
    dict_manager = Manager()
    # measurements_pipelines = dict_manager.dict()
    # measurements_pipelines = {}
    receiving_clients()

