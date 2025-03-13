import os
import stat
from datetime import datetime
from hdbcli import dbapi

run_date = datetime.now().date().strftime("%d-%m-%Y")
date1 = datetime.now().date().strftime("%y%m%d")
now = datetime.now().strftime("%H%M")
run_time = datetime.now().strftime("%H_%M")
seq_no = 12534
date2 = datetime.now().date().strftime("%Y%m%d")
reqfolder = f"RE_report2"


def format_date(date):
    d = str(date)
    d = d[:10]
    return d


query = f"""
    SELECT T0."DocNum" as "PO No", T0."DocDate" as "PO Date",
    '' as "Sold To ID",
    T1."U_COUNTRY" AS "Ship To as per COO", T1."U_UTL_RENESCMNO" as "End Customer",
    T1."U_UTL_PMAprvl" as "Management Approval",
    T1."LineNum" as "Line Number",T2."U_SHIPITEM" as "S&D (Y/N)",
    T1."ItemCode",T1."Quantity" as "Quantity",T0."PriceBefDi" AS "Buy Price",T1."U_CRDDate",T0."CreateDate" AS "Create Date",T0."UpdateDate" as "change Date"

    FROM "RABYTE_PTE_LIVE".OPOR T0
    INNER JOIN "RABYTE_PTE_LIVE".POR1 T1 ON T1."DocEntry"=T0."DocEntry"
    LEFT JOIN "RABYTE_PTE_LIVE".OITM  T2 ON T2."ItemCode"=T1."ItemCode"

    WHERE T1."LineStatus"='O'
    """

try:
    # set the connection detalis
    connection = dbapi.connect(
        address="103.25.172.160",
        port=30015,
        database="RABYTE_PTE_LIVE",
        user="SYSTEM",
        password="Data1234",
    )
    print("🟢🟢 Connection Successful")
    cnx = connection.cursor()
    result = cnx.execute(query)

    col_names = [desc[0] for desc in cnx.description]

    item = cnx.fetchall()

    po_dict = {}
    po_item_dict = {}

    if len(item) == 0:
        print("🔴🔴 No data available yet 🔴🔴")
    else:
        print("✅✅ Data fetched ✅✅\n")
        print("Available PO Number: ")
        for pos in item:
            po_num = pos[0]
            # print(po_num, end=" ")
            line_data = {
                "po_date": pos[1],
                "po_num": pos[0],
                "qty": pos[9],
                "price": pos[10],
                "mpn": pos[8],
                "p_name": pos[4],
                "crd": pos[11],
                "sold_to": pos[2],
                "ship_to": pos[3] 
            }
            if po_num not in po_dict:
                po_dict[po_num] = []

            if po_num not in po_item_dict:
                po_item_dict[po_num] = []

            if line_data not in po_dict[po_num]:
                po_dict[po_num].append(line_data)

            po_item_dict[po_num].append(pos)

        cnx.close()

        if not os.path.exists(reqfolder):
            os.makedirs(reqfolder)
            os.chmod(reqfolder, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

        for po_number, line_items in po_dict.items():
            po_details = line_items
            po_date = format_date(po_details[0]["po_date"])
            sold_to = po_details[0]["sold_to"]
            ship_to = po_details[0]["ship_to"]
            file_name = (
                f"{reqfolder}/PO850_ISIL01_PO_{po_number}_{run_date}_{run_time}.txt"
            )
            with open(file_name, "w") as file:
                file.write(
                    f"ISA*00*          *00*          *ZZ*RABYTEEDI      *ZZ*ISIL01         *{date1}*{now}*U*00401*0000{seq_no}*0*P*>]\n"
                )
                file.write(f"GS*PO*RABYTEEDI*ISIL01*{date2}*{now}*{seq_no}*X*004010]\n")
                file.write(f"ST*850*{seq_no}]\n")
                file.write(f"BEG*00*SA*{po_number}**{po_date}]\n")
                file.write(f"N1*ST*{ship_to}*92*{sold_to}]\n")
                index = 1
                qty_sum = 0
                line_count = 5
                for data in po_item_dict[po_number]:
                    qty = data[9]
                    price = "%.3f" % data[10]
                    part_number = data[8]
                    partner_name = data[4][:30]
                    crd = format_date(data[11])
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

except:
    print("🔴🔴 Connection unsuccessful")
