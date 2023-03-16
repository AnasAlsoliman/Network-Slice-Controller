import csv

# import scope_api as sc

def summation():
    # path = "/home/thunderspirits/Desktop/to_do/test_masks/slice_allocation_mask_tenant_"
    path = "/root/radio_code/scope_config/slicing/slice_allocation_mask_tenant_"
    extension = ".txt"
    masks = [0, 1, 2]
    all_masks = []
    empty_mask = "00000000000000000"
    result = []
    for bit in empty_mask:
        result.append(int(bit))

    for m in masks:
        mask_file = open(path + str(m) + extension)
        mask = mask_file.read()
        mask = mask[:17]
        for i in range(0, len(empty_mask)):
            bit = result[i] + int(mask[i])
            result[i] = bit
        # all_masks.append(mask)
        mask_file.close()

    # print(result)
    full_mask = ""
    for i in result:
        full_mask += str(i)
    print("Summation:", full_mask, "size:", len(result))


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

summation()
