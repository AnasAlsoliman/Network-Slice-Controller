
import scope_api as sc


all_users = sc.ppr_read_metrics()
# print(all_users)
for user, metrics_list in all_users.items():
    print("########## User", user, "##########")

    print("slice_id:", metrics_list['slice_id'])
    print("slice_prb:", metrics_list['slice_prb'])
    print("scheduling_policy:", metrics_list['scheduling_policy'])

    print("sum_requested_prbs:", metrics_list['sum_requested_prbs'])
    print("sum_granted_prbs:", metrics_list['sum_granted_prbs'])

    print("tx_pkts downlink:", metrics_list['tx_pkts downlink'])
    print("rx_pkts uplink:", metrics_list['rx_pkts uplink'])

    print("dl_buffer [bytes]:", metrics_list['dl_buffer [bytes]'])
    print("ul_buffer [bytes]:", metrics_list['ul_buffer [bytes]'])

    print("tx_brate downlink [Mbps]:", metrics_list['tx_brate downlink [Mbps]'])
    print("rx_brate uplink [Mbps]:", metrics_list['rx_brate uplink [Mbps]'])





    # for header, metric in metrics_list.items():
    #     print(header)
    #     print(metric)
    #     print('-----------')
