import socket
from socket import timeout as socket_timeout
import time
import json
from multiprocessing import Process, Queue, Value
import queue


def get_json_file(json_path):
    while True:
        try:
            config_file = open(json_path)
            node_parameters = json.load(config_file)
            config_file.close()
            return node_parameters
        except json.decoder.JSONDecodeError:
            print("failed to open (", json_path, "). Will try again.")


def get_my_info():
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["this_is_my_node"]:
            return node_dict

def get_bs_col_ip():
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["node_type"] == "bs":
            col_ip = node_dict["col_ip"]
            return col_ip


def unpack_header(message):
    global traffic_measurements
    # print("message size:", len(message))
    # message_size includes the 30-bytes header.
    timestamp = message[:20]
    timestamp = float(timestamp)
    # current_time = time.time()
    delay = (time.time() - timestamp)*1000
    delay = round(delay, 5)

    sequence_number = message[20:30]
    # print("new message_size", message_size)
    sequence_number = int(sequence_number)
    # print("sequence_number", sequence_number)
    traffic_measurements.put([len(message), delay, sequence_number])
    return sequence_number
    # return message_size - header_size


# Start receiving injection traffic from the server via the LTE network.
def start_receiving_traffic(traffic_socket):

    counter = 0
    # sequence = 0
    total_received = 0
    current_time = time.time()

    while True:
        try:
            traffic_socket.settimeout(5)
            data, addr = traffic_socket.recvfrom(MTU)
            sequence_number = unpack_header(data)

            if len(data) == 0:  # TODO: not reliable (UDP) and must be changed.
                print("socket closed")
                print("received", counter, "messages")
                traffic_socket.close()
                kill_switch.value = True  # Inform the main process to close me and exit the program.
                break

            total_received = total_received + len(data)
            counter = counter + 1

            if (time.time() - current_time) > 1:
                current_traffic = total_received * 8 / 1000000
                current_traffic = '{0:.3f}'.format(current_traffic)
                print(current_traffic, "Mbits received within", '{0:.7f}'.format(time.time() - current_time),
                      "seconds - current sequence number:", sequence_number)
                total_received = 0
                current_time = time.time()

        except socket_timeout:
            if kill_switch.value:
                print("server is closed (injection)")
                traffic_socket.close()
                return
            print("Process hung for 5 seconds.")
            # restart_switch.value = True  # Inform the main process to restart me.
            # traffic_socket.close()
            # return
        except KeyboardInterrupt:
            restart_switch.value = True  # Inform the main process to restart me.
            traffic_socket.close()
            print("closing receiver process")
            return


# Sending measurement readings back to the server via the Colosseum's internal network.
def start_measurement():

    global traffic_measurements

    node_dict = get_my_info()
    imsi = node_dict["ue_imsi"]
    my_lte_ip = node_dict["lte_ip"]
    # my_col_ip = node_dict["col_ip"]

    while True:
        try:
            measurements_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            measurements_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            measurements_socket.connect((server_col_ip, server_col_port))
            break
        except ConnectionRefusedError:
            if restart_switch.value or kill_switch.value:
                print("Measurement server already closed. Exiting process.")
                return
            print("We will try again shortly (measurements_socket)")
            time.sleep(1)
        except OSError:
            print("Server LTE IP is not ready yet (measurements_socket)")
            time.sleep(1)

    print("connected to main server for measurement on Colosseum IP", server_col_ip)

    imsi = imsi[13:]
    traffic_port = 9900 + int(imsi)  # Every UE will have a dedicated UDP process/port using UE's last 2 IMSI digits.
    traffic_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    traffic_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    print("lte_ip", server_lte_ip)

    traffic_socket.bind((my_lte_ip, traffic_port))

    # Starts a separate process to receive LTE traffic.
    receiving_process = Process(target=start_receiving_traffic, args=(traffic_socket,))
    receiving_process.start()

    # counter = 0
    # simple_msg = "abcd"

    previous_sequence = 0

    check_connectivity = False

    while True:
        if restart_switch.value or kill_switch.value:
            print("Closing measurement socket.")
            measurements_socket.close()
            traffic_socket.close()
            return
        try:
            # counter = counter + 1
            # str_counter = str(counter)
            # padding = "0" * (6 - len(str_counter))
            # str_counter = padding + str_counter
            # msg = bytes(simple_msg + str_counter, 'utf')

            if check_connectivity:  # We need this check here because we have queue.get() before send().
                measurements_socket.send(bytes("hello67890123456789012345678901", 'utf'))
                check_connectivity = False

            measurements = traffic_measurements.get(timeout=5)
            # if measurements is None:
            # # if measurements == "exit":
            # #     print("closing measurement socket via 'exit' flag")
            #     print("Queue timed out. Closing measurement socket")
            #     break

            packet_size = measurements[0]  # In bytes.
            delay = measurements[1]  # In milliseconds.
            sequence = measurements[2]  # To count how many packets were lost.

            # print("sequence", sequence)

            packet_size = str(packet_size)
            assert len(packet_size) <= 6  # packet_size should not exceed 6 digits (1MB).
            packet_size = '0' * (6 - len(packet_size)) + packet_size

            delay = str(delay)
            assert len(delay) <= 16  # delay should not exceed 16 digits.
            delay = delay + '0' * (16 - len(delay))

            lost_packets = sequence - previous_sequence - 1
            assert sequence > previous_sequence  # Looping sequence not implemented yet.
            previous_sequence = sequence
            lost_packets = str(lost_packets)
            assert len(lost_packets) <= 6  # lost_packets should not exceed 6 digits (1M lost packets).
            lost_packets = '0' * (6 - len(lost_packets)) + lost_packets

            report = "_" + packet_size + "_" + delay + "_" + lost_packets
            # print(report)
            report = bytes(report, 'utf')

            measurements_socket.send(report)
            # print("sending", msg)
            # time.sleep(1)
        except KeyboardInterrupt:
            print("\nctrl+c from start_measurement")
            break
        except BrokenPipeError:
            print("server is closed")
            break
        except queue.Empty:
            print("Queue timed out. Checking connectivity...")
            check_connectivity = True
        except OSError:
            print("IP address might be in use")
            time.sleep(0.5)
            break

    measurements_socket.close()
    traffic_socket.close()
    kill_switch.value = True


if __name__ == "__main__":

    server_lte_ip = "172.16.0.1"

    server_col_ip = get_bs_col_ip()
    server_col_port = 5555

    restart_switch = Value('i')
    restart_switch.value = False

    kill_switch = Value('i')
    kill_switch.value = False

    MTU = 50000

    while True:

        try:
            traffic_measurements = Queue()
            restart_switch.value = False

            measurements_sending_thread = Process(target=start_measurement)
            measurements_sending_thread.start()
            measurements_sending_thread.join()

            # Exiting the entire program when the server closes.
            if kill_switch.value:
                print("terminating entire client process")
                break

            # Traffic process got hung so we need to restart the process.
            print("refreshing client process")

        except KeyboardInterrupt:
            # A better option is to close the recv() socket in case of the server is connected but idle (although the
            # server is designed to never be idle).
            kill_switch.value = True
            # delay_measurements.put("exit")  # I don't think it is really needed.
            break

    print("terminating program...")








