import json
import sys
import time
import numpy as np
# import pandas as pd
import scope_api as sc
from DQN import *
from preprocess import *
from threading import Thread
from multiprocessing import Process, Queue, Manager, Value
import csv



def generate_traffic_seasonality_coeff():
    dayINweek_loads=np.array([.93,.999,.97,.97,.96,.88,.75])
    #dayINweek_loads=np.array([.99,.75,.7,.8,.85,.9,.95])
    #dayINweek_loads=np.array([.93,.999,.97,.88,.75,.7,.65])
    poly_nomial_order=6
    tao=np.zeros((7,poly_nomial_order))
    for t in range(7):
        tao[t,:]=np.power(t, np.arange(poly_nomial_order))
    b=np.linalg.lstsq(tao,dayINweek_loads)[0]
    
    hoursINday_loads=np.array([0.44,.31,.2,.16,.12,.17,.29,.52,.71,.82,.85,.84,.83,.82,.82,.82,.81,.81,.84,.88,1,.98,.93,.67,0.44])
    poly_nomial_order=12
    t_matrix=np.zeros((25,poly_nomial_order))
    for t in range(25):
        t_matrix[t,:]=np.power(t, np.arange(poly_nomial_order))

    a=np.linalg.lstsq(t_matrix,hoursINday_loads)[0]
    return a,b

def v1(t,a):
  t_vec_powers= np.power(t, np.arange(a.shape[0]))
  return np.dot(t_vec_powers,a)

def v2(tao,b):
  tao_vec_powers= np.power(tao, np.arange(b.shape[0]))
  return np.dot(tao_vec_powers,b)

def V_func(t,eta,a,b):
  return eta*v1(t%24,a)*v2(int(t/24)%7,b)


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


def get_json_file_node(node_id):
    config_file = open("nodes_config_files/" + node_id + "_config_file.txt")
    while True:
        try:
            node_parameters = json.load(config_file)
            break
        except json.decoder.JSONDecodeError:
            print("failed to open config file of node", node_id, "we will try again.")
    config_file.close()

    return node_parameters


def update_model_action(node_id, action, action_values, loss, loss_target, reward):

    model_dict = {}
    model_dict["action"] = int(action)
    model_dict["action_values"] = action_values
    model_dict["loss"] = float(loss)
    model_dict["reward"] = float(reward)
    model_dict["loss_target"] = float(loss_target)

    # print("action", action)
    # print("action_values", action_values)
    # print("reward", reward, "from node", node_id)

    node_file_path = "nodes_config_files/" + node_id + "_model_file.txt"
    while True:
        try:
            config_file = open(node_file_path, "w")
            json.dump(model_dict, config_file, indent=4)
            config_file.close()
            break
        except TypeError:
            print("Failed to write", node_file_path, "will try again")
            # print(model_dict)
            # print("action_values[0] type:", type(action_values[0]))
            # print(action_values[0])
            # print("action type:", type(action))
            # print(action)
            # time.sleep(1)

def set_PRBs(node_id,history_size,RB_allocation_period,n_actions,agent,t):
    # my_start_time = time.time()

    # config_files_path = "nodes_config_files/"
    # node_file_path = config_files_path + node_id + "_config_file.txt"
    # config_file = open(node_file_path)
    # node_parameters = json.load(config_file)
    # config_file.close()

    node_parameters = get_json_file_node(node_id)

    # Read current available resources.
    slice_id = node_parameters["my_slice_id"]
    mask, current_prb = get_mask_size(slice_id)

    # # Read current available resources.
    # slice_prb = node_parameters["my_slice_resources"]

    # Compute ratio.
    if int(node_parameters['sum_granted_prbs']) == 0:
        ratio = 1
    else:
        ratio = int(node_parameters['sum_requested_prbs']) / int(node_parameters['sum_granted_prbs'])
    
    if(t<history_size+RB_allocation_period):
        a=choose_action(ratio)
    else:
        data_df = pd.read_csv("/root/ue_datasets/temp_ue_" + node_id + ".csv").iloc[-(RB_allocation_period+history_size):,:]
        #a ,action_values,loss,reward=choose_action_and_train_model(data_df,RB_allocation_period,n_actions,agent,t)
        a,action_values,loss,reward,loss_target  = choose_action_and_train_model_updated(data_df,RB_allocation_period,history_size,agent)
        update_model_action(node_id, a, action_values.tolist(), loss,loss_target, reward)
        # print("node_id:", node_id, "- slice_prb:", slice_prb, "- prb modifier:", a)
    requested_prb = int(current_prb + a)
    if requested_prb < 1:
        requested_prb = 1

    # print("set_PRBs exc time =", '{0:.8f}'.format((time.time() - my_start_time)*1000), "ms")

    return current_prb, requested_prb



# def update_traffic_parameters(node_id, packet_size, max_traffic_size, slice_prb):
#     # config_files_path = "nodes_config_files/"
#
#     # config_file = open(node_file_path)
#     # node_parameters = json.load(config_file)
#     # config_file.close()
#
#     node_parameters = get_json_file_node(node_id)
#
#     # Read current available resources.
#     slice_id = node_parameters["my_slice_id"]
#
#     slice_prb = int(slice_prb)
#
#     if slice_prb < 1:
#         slice_prb = 1
#
#     # print("Node", node_id, "slice_id", slice_id, "requested PRBs:", slice_prb)
#
#     # Request the resources from SCOPE.
#     # sc.write_slice_allocation_relative(slice_id, slice_prb)
#     resource_request_queue.put([slice_id, slice_prb])
#
#     # Read how much we actually got.
#     mask, prb = get_mask_size(slice_id)
#
#     r = np.random.uniform(low=0.0, high=1.0, size=None)
#     desired_bandwidth = max_traffic_size * r
#     #desired_bandwidth=np.clip(max_traffic_size * r, packet_size, max_traffic_size)
#
#     # Update only 4 attributes.
#     traffic_parameters = {}
#     traffic_parameters["packet_size"] = packet_size
#     traffic_parameters["desired_bandwidth"] = desired_bandwidth
#     traffic_parameters["my_slice_resources"] = prb
#     traffic_parameters["requested_slice_resources"] = slice_prb
#
#     # Only write on your designated files.
#     node_file_path = "nodes_config_files/" + node_id + "_traffic_file.txt"
#     while True:
#         try:
#             config_file = open(node_file_path, "w")
#             json.dump(traffic_parameters, config_file, indent=4)
#             config_file.close()
#             # print("traffic_parameters", traffic_parameters)
#             break
#         except TypeError:
#             print("Failed to write", node_file_path, "will try again")
#             print(traffic_parameters)
#             # sys.exit("closing...")
#
#     # print("Node", node_id, "PRBs:", slice_prb)
#     # print("Slice", slice_id, "mask:", mask, "with size:", prb)
#     # print("Node", node_id, "packet size:", packet_size, "bytes")
#     # print("Node", node_id, "bandwidth:", desired_bandwidth * 8 / 1000000, "Mbps")
#     # print("Therefore, expected packet inter-arrivals: ", packet_size / desired_bandwidth, "second")
#     # print("----------------------------------")


def update_traffic_parameters_daily_pattern(node_id, packet_size, traffic_params, current_prb, requested_prb,t,a,b):

    node_parameters = get_json_file_node(node_id)

    # Read current available resources.
    slice_id = node_parameters["my_slice_id"]

    # Request the resources from SCOPE.
    # sc.write_slice_allocation_relative(slice_id, slice_prb)
    resource_request_queue.put([slice_id, requested_prb])

    L=24
    eta=traffic_params[0]
    bias=traffic_params[1]
    v=V_func(int((t+bias)/L),eta,a,b)
    noise=np.random.uniform(.8,1.2)
    desired_bandwidth = v * noise
    #desired_bandwidth=np.clip(max_traffic_size * r, packet_size, max_traffic_size)

    # Update only 4 attributes.
    traffic_parameters = {}
    traffic_parameters["packet_size"] = packet_size
    traffic_parameters["desired_bandwidth"] = desired_bandwidth
    traffic_parameters["my_slice_resources"] = current_prb
    traffic_parameters["requested_slice_resources"] = requested_prb

    # Only write on your designated files.
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
            print("requested_prb:", requested_prb, "- type:", type(requested_prb))
            time.sleep(0.2)
            # sys.exit("closing...")

    # print("Node", node_id, "PRBs:", slice_prb)
    # print("Slice", slice_id, "mask:", mask, "with size:", prb)
    # print("Node", node_id, "packet size:", packet_size, "bytes")
    # print("Node", node_id, "bandwidth:", desired_bandwidth * 8 / 1000000, "Mbps")
    # print("Therefore, expected packet inter-arrivals: ", packet_size / desired_bandwidth, "second")
    # print("----------------------------------")
def get_mask_size(slice_id):
    path = "/root/radio_code/scope_config/slicing/slice_allocation_mask_tenant_"
    extension = ".txt"
    mask_file = open(path + str(slice_id) + extension)
    mask = mask_file.read()
    mask_file.close()
    # mask = mask[:17]
    counter = 0
    for b in mask:
        if b == "1":
            counter += 1

    return mask, counter


def choose_action(ratio):
    r = np.random.uniform(low=0.0, high=1.0, size=None)
    action = 0
    actions_list = [-3, -2, -1, 0, 1, 2, 3]
    if (r > .2):

        if (ratio >= 2):
            action = 3
        elif (1.5 <= ratio < 2):
            action = 2
        elif (1.1 <= ratio < 1.5):
            action = 1
        elif (.9 <= ratio < 1.1):
            action = 0
        elif (.7 <= ratio < .9):
            action = -1
        elif (.3 <= ratio < .7):
            action = -2
        elif (ratio < .3):
            action = -3
    else:
        action = actions_list[np.random.randint(0, len(actions_list), 1)[0]]
    return action



def choose_action_from_model(data_df,std,mean,RB_allocation_period,n_actions,agent):
    state_df=state_extraction_test(data_df,std,mean)
    state=np.expand_dims(state_df.values,axis=0)
    action_values=agent.get_action_values(state)
    action=convert_action(np.argmax(action_values))

    return action,action_values


def PRB_Slice_controller(PRB_arr, max_allowed):
    return (PRB_arr / np.sum(PRB_arr) * max_allowed).astype(int)


def choose_action_and_train_model(data_df,RB_allocation_period,n_actions,agent,t):
    loss = 0
    state=state_extraction_test(data_df).values
    state=state.reshape(state.shape[0]*state.shape[1])
    action_values=agent.get_action_values(np.expand_dims(state,axis=0))[0]
    action=agent.choose_eps_greedy_action(action_values)
    reward_arr=compute_reward_noclip(data_df.iloc[-RB_allocation_period:,:])
    reward,state=agent.update_stats_reward_state(reward_arr,state)
    if(agent.buffer_count>0):
        agent.add_reward_next_state_buffer(reward,state)
        loss=agent.learn()
        loss = loss.detach().numpy().item()
    agent.add_state_action_buffer(state,action)
    a=convert_action(action)
    return a,action_values,loss,reward


def choose_action_and_train_model_updated(data_df,RB_allocation_period,history_size,agent):

    state      = state_extraction_min_max_scale(data_df.iloc[-history_size-RB_allocation_period:-RB_allocation_period,:]).values
    next_state = state_extraction_min_max_scale(data_df.iloc[-history_size:,:]).values

    state=state.reshape(state.shape[0]*state.shape[1])
    next_state=next_state.reshape(next_state.shape[0]*next_state.shape[1])
    
    last_action=unconvert_action(data_df['action'].values[-1])
    action_values,loss_target=agent.get_action_values(np.expand_dims(next_state,axis=0))
    action=agent.choose_eps_greedy_action(action_values)
    a=convert_action(action)
    
    
    reward=np.mean(compute_reward_delay_based(data_df.iloc[-RB_allocation_period:,:]))
    
    #if(check_states(data_df.iloc[-history_size:,:])):
    agent.add_experience_buffer(state,last_action,reward,next_state)
        
    loss=agent.learn()
    loss = loss.detach().numpy().item()
    
    return a,action_values,loss,reward,loss_target


def start_an_agent(node_id, traffic_params):

    a,b=generate_traffic_seasonality_coeff()
    MAX_PRB = 17
    RB_allocation_period = 1
    history=5
    num_of_time_slots = 30000
    expected_experiment_time = num_of_time_slots/4/60
    history_size=history*RB_allocation_period
    n_actions=7
    
    agent = DQNAgent(gamma=.9, lr=1e-4, n_actions=n_actions, state_dim=7, history_window=history_size,batch_size=64,initial_eps=.5,final_eps=0.05,eps_period=25000,tau=0.001)
    #agent.load_models('/root/radio_api/files/model_'+i+'.pth')
    # std=pd.read_pickle('/root/radio_api/files/std_'+i+'.pkl')
    # mean=pd.read_pickle('/root/radio_api/files/mean_'+i+'.pkl')
    # agent.load_initial_mean_std(mean.values,std.values)

    dataset_file = open("/root/ue_datasets/ue_" + node_id + ".csv")
    dataset_reader = csv.reader(dataset_file)
    temp_dataset = []

    # Adding the header to temp_dataset.
    dataset_header = next(dataset_reader)
    temp_dataset.append(dataset_header)

    new_entry = next(dataset_reader)

    # Initialize temp_dataset by copying the first entry 20 times.
    for entry in range(0, history_size+RB_allocation_period):
        temp_dataset.append(new_entry)

    # Fill up temp_dataset with the last 20 entries.
    while True:
        try:
            new_entry = next(dataset_reader)
            temp_dataset.append(new_entry)
            del temp_dataset[1]  # Keeping the header.
        except StopIteration:
            break

    # timestamp = float(new_entry[0])
    current_time = time.time()
    start_time = time.time()
    slot_number = 1
    previous_timestamp = 0

    temp_dataset_path = "/root/ue_datasets/temp_ue_" + node_id + ".csv"

    try:
        while slot_number < num_of_time_slots:
            try:
                # If the next line fails, the rest of the 'try' block will be skipped.
                new_entry = next(dataset_reader)
                temp_dataset.append(new_entry)
                del temp_dataset[1]  # Keeping the header.

                with open(temp_dataset_path, "w") as temp_dataset_file:
                    temp_dataset_writer = csv.writer(temp_dataset_file)
                    temp_dataset_writer.writerows(temp_dataset)

                #######################################
                ########## DRL code goes here #########
                #######################################

                current_timestamp = int(new_entry[0])
                
                if (slot_number % RB_allocation_period == 0):
                    current_prb, requested_prb = set_PRBs(node_id, history_size, RB_allocation_period, n_actions, agent, slot_number)
                    # if (np.sum(PRB_arr) > MAX_PRB):
                    #     PRB_arr = PRB_Slice_controller(PRB_arr, MAX_PRB)
                    #update_traffic_parameters(node_id, 1000, max_traffic_size, PRB)
                    sleep_time = 0.220 - (time.time() - current_timestamp/1000)
                    if sleep_time < 0:
                        sleep_time = 0
                    time.sleep(sleep_time)
                    update_traffic_parameters_daily_pattern(node_id, 1000, traffic_params, current_prb, requested_prb ,slot_number,a,b)

                slot_duration = '{0:.8f}'.format((time.time() - current_time) * 1000)
                time_passed = '{0:.8f}'.format((time.time() - start_time) / 60)
                time_remaining = expected_experiment_time - float(time_passed)

                # print("Node ID:", node_id, "- entry inter-arrival:", float(new_entry[0]) - timestamp,
                #       "ms - entry number:", dataset_reader.line_num, "- slot_number:", slot_number, "- slot_duration:",
                #       slot_duration, "ms - temp_dataset size =", len(temp_dataset))

                # print("Node ID:", node_id, "- imsi:", new_entry[2],
                #       "ms - entry number:", dataset_reader.line_num, "- slot_number:", slot_number, "- slot_duration:",
                #       slot_duration, "ms - delays =", temp_dataset[-1][34])

                # print("Node ID:", node_id, "- imsi:", new_entry[2],
                #       "ms - entry number:", dataset_reader.line_num, "- first delays:", temp_dataset[0][34],
                #       "- last delays:", temp_dataset[-1][34])

                # timestamp_difference = current_timestamp - previous_timestamp
                # print("Node ID:", node_id, "- slot_number:", slot_number,"- slot_duration:", slot_duration, "ms - time passed:", time_passed, "minutes - requested resources:", requested_prb, "- timestamp_difference:", timestamp_difference)
                # print("Node ID:", node_id, "- slot_number:", slot_number,"- slot_duration:", slot_duration, "ms - time passed:", time_passed, "minutes - requested resources:", requested_prb, "- time_remaining:", time_remaining, "minutes")
                print("Node ID:", node_id, "- slot_number:", slot_number, "- slot_duration:", slot_duration,
                      "ms - time passed:", time_passed, "minutes - time_remaining:", time_remaining, "minutes", "processing time:", sleep_time)
                # print("Node ID:", node_id, "- slot_number:", slot_number, "- slot_duration:", slot_duration,
                #       "ms - time passed:", time_passed, "minutes - time_remaining:", time_remaining, "minutes",
                #       "scope_time:", current_timestamp, "- my time:", time.time())


                # timestamp = float(new_entry[0])
                current_time = time.time()
                slot_number = slot_number + 1
                previous_timestamp = current_timestamp

            # No new entry was found. Will try again.
            except StopIteration:
                time.sleep(0.01)
    except KeyboardInterrupt:
        print("\nCtrl+C detected on node", node_id)

    # Save model here.
    agent.save_models('/root/radio_api/files/trained_online_model_'+node_id+'.pth')
    # np.save('/root/radio_api/files/mean_state_agent_'+node_ids[i],agent_list[i].mean_state)
    # np.save('/root/radio_api/files/std_state_agent_'+node_ids[i],agent_list[i].std_state)
    print("Model saved for agent", node_id)
    resource_request_queue.put("exit")


def start_slice_resource_manager():

    # masks_path = "/root/radio_code/scope_config/slicing/slice_allocation_mask_tenant_"

    # slice_id 2 (third slice) is hard-coded into scope and we need to manually free its resources.
    sc.write_slice_allocation_relative(2, 0)

    # freeing slice_id 1 (second slice) as well if we want a single slice in the whole BS.
    sc.write_slice_allocation_relative(1, 0)

    while True:
        try:
            request = resource_request_queue.get()
            if request == "exit":
                break
            slice_id = request[0]
            requested_prb = request[1]
            # requested_prb = 8
            sc.write_slice_allocation_relative(slice_id, requested_prb)
            # mask, prb = get_mask_size(slice_id)
            # print("Slice", slice_id, "mask:", mask, "with size:", prb)
        except KeyboardInterrupt:
            print("\nCtrl+C detected on slice manager")
    print("terminating the slice manager")


if __name__ == "__main__":

    resource_request_queue = Queue()
    
    
    eta1=3e6/8
    eta2=3e6/8
    
    bias1=0
    bias2=24*7

    # slice_id 2 (third slice) is hard-coded into scope and we need to manually free its resources.
    # sc.write_slice_allocation_relative(2, 1)

    # traffic_params1=[eta1,bias1]
    # traffic_params2=[eta2,bias2]
    #
    # Max_Traffic_size1 = 100000
    # Max_Traffic_size2 = 200000

    # traffic_params1 = [eta1, bias1]
    # Max_Traffic_size1 = 100000
    traffic_params1 = [eta2,bias2]
    Max_Traffic_size1 = 200000

    try:
        slice_manager = Process(target=start_slice_resource_manager)
        slice_manager.start()
        agent_1 = Process(target=start_an_agent, args=("2", traffic_params1,))
        agent_1.start()
        # agent_2 = Process(target=start_an_agent, args=("3", traffic_params2,))
        # agent_2.start()

        slice_manager.join()
        agent_1.join()
        # agent_2.join()

    except KeyboardInterrupt:
        print("\nCtrl+C detected on main process")
        # Gives some time for the process to exit (save models).
        time.sleep(5)


