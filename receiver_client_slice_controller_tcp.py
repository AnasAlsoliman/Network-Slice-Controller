
'''
Version 8
This version is the same as v7 but with all the fixes and works on SCOPE.
'''


import socket
# import threading
from multiprocessing import Process, Queue, Value
import select
import json
import time
# from queue import Queue
from datetime import datetime


def get_json_file(json_path):
    while True:
        try:
            config_file = open(json_path)
            node_parameters = json.load(config_file)
            config_file.close()
            return node_parameters
        except json.decoder.JSONDecodeError:
            print("failed to open (", json_path, "). Will try again.")


def get_injection_parameters(node_id):
    # json_file = open("nodes_parameters_file.txt")
    # nodes_parameters = json.load(json_file)
    # json_file.close()

    # nodes_parameters = get_json_file("nodes_parameters_file.txt")
    # packet_size = nodes_parameters[node_id]["packet_size"]
    # desired_bandwidth = nodes_parameters[node_id]["desired_bandwidth"]

    # node_file_path = "nodes_config_files/" + node_id + "_config_file.txt"
    # config_file = open(node_file_path)
    # node_parameters = json.load(config_file)
    # config_file.close()
    # packet_size = node_parameters["packet_size"]
    # desired_bandwidth = node_parameters["desired_bandwidth"]

    # injection_file = open("nodes_config_files/" + node_id + "_traffic_file.txt")
    # traffic_parameters = json.load(injection_file)
    # injection_file.close()

    # while True:
    #     try:
    #         injection_file = open("nodes_config_files/" + node_id + "_traffic_file.txt")
    #         traffic_parameters = json.load(injection_file)
    #         injection_file.close()
    #         break
    #     except json.decoder.JSONDecodeError:
    #         print("failed to open node", node_id, "traffic file. Will try again.")

    traffic_parameters = get_json_file("nodes_config_files/" + node_id + "_traffic_file.txt")

    packet_size = traffic_parameters["packet_size"]
    desired_bandwidth = traffic_parameters["desired_bandwidth"]
    return packet_size, desired_bandwidth


def unpack_header(message):
    global delay_measurements
    # message_size includes the 27-bytes header.
    timestamp = message[:20]
    timestamp = float(timestamp)
    # current_time = time.time()
    delay = (time.time() - timestamp)*1000
    delay = round(delay, 5)

    # ##### Testing whats wrong with float #####
    # print("timestamp", timestamp)
    # print("current_time", current_time)
    # if delay == 1:
    #     delay_measurements.put("exit")
    # if delay > 10000:
    #     delay_measurements.put("exit")

    delay_measurements.put(delay)
    # print("unpack_header delay =", delay, "ms")

    message_size = message[20:27]
    # print("new message_size", message_size)
    message_size = int(message_size)
    return message_size - header_size


def recv_from_socket(client_socket, buffer):
    start_time = time.time()
    while True:
        try:
            message = client_socket.recv(buffer)
            if len(message) == 0:
                print("The server has closed the connection.")
                kill_switch.value = True
                server_kill_switch.value = True
                return ""
            return message

        except BlockingIOError:
            elapsed_time = time.time() - start_time
            if elapsed_time > 5:  # 5 seconds of inactivity.
                print("The process is hanging for over", elapsed_time, "seconds and going to restart")
                kill_switch.value = True
                return ""
            # time.sleep(0.01)  # Check the buffer socket every 0.01 second.


def receive_up_to(client_socket, buffer):

    message = recv_from_socket(client_socket, buffer)
    if kill_switch.value:
        return ""

    while len(message) < buffer:
        remaining_bytes = buffer - len(message)
        remaining_message = recv_from_socket(client_socket, remaining_bytes)
        if kill_switch.value:
            return ""
        message = message + remaining_message
    return message


def start_traffic_injection():

    print("Starting receiving traffic...")

    ########## TCP ##########

    connected = False
    while not connected:
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            client_socket.connect((serverIP, serverPort))
            client_socket.setblocking(False)
            connected = True
        except ConnectionRefusedError:
            # Very unlikely to happen (closing the server at the same time the client decide to restart).
            if kill_switch.value:
                print("Main server already closed. Exiting process.")
                return
            print("We will try again shortly (traffic injection socket)")
            time.sleep(1)
        except TimeoutError:
            if kill_switch.value:
                print("Main server already closed. Exiting process.")
                return
            print("connect timed out. We will try again.")
            time.sleep(1)


    rounds = 0
    current_time = time.time()

    total_received_one_second = 0
    total_received_entire_session = 0
    total_messages = 0

    node_id = get_my_node_id()
    packet_size, desired_bandwidth = get_injection_parameters(node_id)
    sleep_time = packet_size / desired_bandwidth

    print('Packet size: ', packet_size / 1000, "KB")
    print('Desired bandwidth: ', desired_bandwidth * 8 / 1000000, "Mbps")
    print("Packet inter-arrivals: ", sleep_time, "second")



    while True:
        if kill_switch.value:
            print("Terminating the client via kill_switch")
            break
        try:
            # If enabled, the sender would send (and the receiver would receive) exactly
            # 10MB because 1 second / 0.1 sleep time * the receive buffer of 1MB = 10MB
            # time.sleep(0.1)

            # The "recv(buffer_size)" function will return under two conditions:
            # 1- when the buffer receives some data but less than buffer_size, it will wait a bit (for less than a ms)
            # then return whatever it currently has in the buffer.
            # 2- when the buffer receives up to buffer_size bytes, it will return immediately.
            # As far as I can see, Ubuntu returns when it receives up to 23,168 bytes.
            # Looks like Mac can receive more than 1.4 MB in one recv() (tested on localhost).
            # buffer_size = 1000000  # It might be better to set buffer_size to chunk size.
            header = receive_up_to(client_socket, header_size)
            if kill_switch.value:
                break

            message_size = unpack_header(header)
            message_body = receive_up_to(client_socket, message_size)
            if kill_switch.value:
                break

            bytes_received = len(message_body) + header_size

            # Keeping taps on how much received in 1 second and in the entire session.
            total_received_one_second += bytes_received
            total_received_entire_session += bytes_received
            total_messages += 1

            # Some prints every second.
            if time.time() - current_time > 1:
                now = datetime.now()
                now = now.strftime("%H:%M:%S")
                current_traffic = total_received_one_second * 8 / 1000000
                current_traffic = '{0:.3f}'.format(current_traffic)
                elapsed_time = time.time() - current_time
                elapsed_time = '{0:.10f}'.format(elapsed_time)
                print(current_traffic, "Mbits received within", elapsed_time,
                      "seconds - Current Time =", now)

                total_received_one_second = 0
                current_time = time.time()

########### Some prints when injection parameters update ###########
                # new_packet_size, new_desired_bandwidth = get_injection_parameters(node_id)
                # if packet_size == new_packet_size and desired_bandwidth == new_desired_bandwidth:
                #     pass
                # else:
                #     packet_size = new_packet_size
                #     desired_bandwidth = new_desired_bandwidth
                #     sleep_time = packet_size / desired_bandwidth
                #     print("Injection parameters updated for node", node_id, ", Packet size:", packet_size / 1000, "KB",
                #           ', Desired bandwidth: ', desired_bandwidth * 8 / 1000000, "Mbps", ", Packet inter-arrivals: ",
                #           sleep_time, "second")
###################################################################

            rounds += 1

        except KeyboardInterrupt:
            print("\nCtrl+C detected")
            kill_switch.value = True
            break

    client_socket.close()
    delay_measurements.put("exit")
    print("total rounds = " + str(rounds))
    print("total messages =", total_messages)
    print("total session =", total_received_entire_session, "bytes")
    print("total session =", total_received_entire_session / 1000000, "MB")


def get_bs_col_ip():
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["node_type"] == "bs":
            col_ip = node_dict["col_ip"]
            return col_ip


def get_my_imsi():
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["this_is_my_node"]:
            imsi = node_dict["ue_imsi"]
            return imsi


def get_my_node_id():
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["this_is_my_node"]:
            return node_id


def start_sending_measurements():
    global delay_measurements
    imsi = get_my_imsi()
    imsi = imsi[13:]
    measurements_port = 9900 + int(imsi)
    col_ip = get_bs_col_ip()
    print("BS col_ip", col_ip)
    ########## TCP ##########

    time.sleep(0.8)  # for the server to create its measurement socket.

    while True:
        try:
            measurements_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            measurements_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            measurements_socket.connect((col_ip, measurements_port))
            break
        except ConnectionRefusedError:
            if kill_switch.value:
                print("Measurement server already closed. Exiting process.")
                return
            print("We will try again shortly (measurements_socket)")
            time.sleep(2)

    print("connected to measurement server", col_ip, "on personalized port", measurements_port)

    while True:
        if kill_switch.value:
            break
        try:
            # Blocks until the queue has data to get.
            delay_measurement = delay_measurements.get()

            if delay_measurement == "exit":
                print("Closing down the measurements_sending_thread")
                break

            delay_measurement = str(delay_measurement)
            delay_measurement = delay_measurement + "0" * (16 - len(delay_measurement))

            # if len(delay_measurement) < 8:
            #     delay_measurement = delay_measurement + "0" * (8 - len(delay_measurement))

            delay_measurement = bytes(delay_measurement, 'utf')
            sent = measurements_socket.send(delay_measurement)
            # print("delay socket sent",delay_measurement, "with size", sent, "bytes")

        # On localhost, this exception is returned when trying to send() to a closed socket.
        except ConnectionResetError:
            print("Measurement server closed the localhost connection")
            break

        # When implemented over the network, this exception is returned when trying to send() to a closed socket.
        except BrokenPipeError:
            print("Measurement server closed the connection over the network")
            break

        except KeyboardInterrupt:
            print("\nCtrl+C detected on measurement thread")
            break

    measurements_socket.close()


if __name__ == "__main__":

    # time.sleep(10)

    serverIP = '172.16.0.1'
    # serverIP = '192.168.86.50'
    # serverIP = 'srs_spgw_sgi'
    # serverIP = 'localhost'
    serverPort = 9999
    header_size = 27

    kill_switch = Value('i')

    server_kill_switch = Value('i')
    server_kill_switch.value = False

    while True:

        try:
            delay_measurements = Queue()
            kill_switch.value = False

            traffic_injection_thread = Process(target=start_traffic_injection)
            traffic_injection_thread.start()

            measurements_sending_thread = Process(target=start_sending_measurements)
            measurements_sending_thread.start()

            # # I don't really need to join the threads but I did to enable KeyboardInterrupt from the main thread.
            traffic_injection_thread.join()
            measurements_sending_thread.join()

            # Exiting the entire program when the server closes.
            if server_kill_switch.value:
                print("server closed via kill_switch")
                break
            print("refreshing client process")

        except KeyboardInterrupt:
            # A better option is to close the recv() socket in case of the server is connected but idle (although the
            # server is designed to never be idle).
            kill_switch.value = True
            delay_measurements.put("exit")  # I don't think it is really needed.
            break

    print("terminating program...")










