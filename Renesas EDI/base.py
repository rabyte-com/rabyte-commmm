import os
import stat
from datetime import datetime
from hdbcli import dbapi

run_date = datetime.now().date().strftime('%d-%m-%Y')
date1 = datetime.now().date().strftime("%y%m%d")
now = datetime.now().strftime('%H%M')
run_time = datetime.now().strftime('%H_%M')
seq_no = 12534
date2 = datetime.now().date().strftime("%Y%m%d")

# create a connection with SAP LLP server for fetching data
connection = dbapi.connect(
    address="103.25.172.160",
    port=30015,
    database="RABYTE_LLP_LIVE01_0808",
    user="SYSTEM",
    password="Data1234",
)

reqfolder = f"RE_report"
file_name = f"{reqfolder}/PO850_ISIL01_PO_PO_NUMBER_{run_date}_{run_time}.txt"

if not os.path.exists(reqfolder):
    os.makedirs(reqfolder)
    os.chmod(reqfolder, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

pos = [
    (2500, 6.001, "HIP2211FBZ-T", "SMARTEN POWER SYSTEMS PVT LTD after30digits", '20250528'),
    (2105, 3.000, "HS3001", "GENUS SMARTEN POWER SYSTEMS PVT LTD", '20250930'),
    (2995, 0.005, "HS3001QWQE", "SMARTEST GENUS SMARTEN POWER SYSTEMS PVT LTD", '20250930')
]

with open(file_name, "w") as file:
    file.write(
        f"ISA*00*          *00*          *ZZ*RABYTEEDI      *ZZ*ISIL01         *{date1}*{now}*U*00401*0000{seq_no}*0*P*>]\n"
    )
    file.write(
        f"GS*PO*RABYTEEDI*ISIL01*{date2}*{now}*{seq_no}*X*004010]\n"
    )
    file.write(
        f"ST*850*{seq_no}]\n"
    )
    file.write(f"BEG*00*SA*<PO_NUMBER>**<PO_DATE>]\n")
    file.write(f"N1*ST*<SOLD_TO>*92*<SHIP_TO>]\n")
    index = 1
    qty_sum = 0
    line_count = 5
    for data in pos:
        qty = data[0]
        price = "%.3f" % data[1]
        part_number = data[2]
        partner_name = data[3][:30]
        crd = data[4]
        file.write(
            f"PO1*{index}*{qty}*EA*{price}*CT*VP*{part_number}*BP*{partner_name}]\n"
        )
        file.write(f"REF*ZZ*{partner_name}]\n")
        file.write(f"SCH*{qty}*EA***002*{crd}]\n")
        index += 1
        qty_sum += qty
        line_count += 3
    file.write(f"CTT*{index-1}*{qty_sum}]\n")
    file.write(f"SE*{line_count}*{seq_no}]\n")
    file.write(f"GE*1*{seq_no}]\n")
    file.write(f"IEA*1*0000{seq_no}]\n")

    print("🟢🟢 Done exporting!!")
