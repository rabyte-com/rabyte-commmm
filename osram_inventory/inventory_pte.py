from hdbcli import dbapi
from datetime import datetime
import random


def transform_to_edifact_structure(cursor):
    edifact_messages = []

    # Add UNA segment for service string advice
    una_segment = "UNA:+.? '"

    # Generate current date and time for UNB segment
    current_datetime = datetime.now()
    date_of_preparation = current_datetime.strftime("%y%m%d")
    time_of_preparation = current_datetime.strftime("%H%M")
    # unique_ref1 = datetime.now().strftime('%Y%m%d%H%M%S')+ str(random.randint(10000, 9999))
    unique_ref = datetime.now().strftime("%y%m%d") + str(random.randint(1000, 9999))
    unique_ref1 = datetime.now().strftime("%Y%m%d%H%M%S")
    partnermailbox = "RabyteSG"
    dbid = "512331"  # NAD segment for distributor (static example)
    dbname = "Rabyte SG"  # NAD segment for buyer (static example)
    cux = "USD"  # CUX segment for currency details (static example)
    Despatch_advice_number = ""

    # Add UNB and UNH segments for interchange header
    unb_unh_segments = (
        f"UNB+UNOH:1+{partnermailbox}:ZZ+OSRAMOSPENTEST+{date_of_preparation}:{time_of_preparation}+{unique_ref}++INVRPT'\n"
        f"UNH+{unique_ref}+INVRPT:D:97A:UN'\n"
        f"BGM+35+{current_datetime.strftime('%Y%m%d%H%M%S')}+9'\n"  # Unique message ID based on timestamp
        f"DTM+137:{current_datetime.strftime('%Y%m%d')}:102'\n"  # DTM segment for report date
        f"DTM+366:{current_datetime.strftime('%Y%m%d')}:102'\n"  # DTM segment for inventory date
        f"NAD+BY+{dbname}::92'\n"
        f"NAD+DB+{dbid}::91'\n"
        f"CUX+2:{cux}:10'\n"  # CUX segment for currency details (static example)
    )

    # Loop through DataFrame rows to create messages
    # for index, row in df.iterrows():
    index = 0
    for data in cursor:
        Company_Code = data[0]
        Material_Code = data[1]
        Material_Des = data[2]
        SandD_YorN = data[3]
        Supplier_Code = data[4]
        Make = data[5]
        Make_Div = data[6]
        Stock_Qty = data[7]
        UOM = data[8]
        Avg_Price = data[10]
        Last_Receipt_Date = data[9]
        message = [
            f"LIN+{index+1}++{Material_Code}:VP::91'",  # LIN segment for line item
            f"INV++++2'",  # INV segment for inventory management details
            f"QTY+17:{Stock_Qty}:PCE'",  # QTY segment for quantity
            f"PRI+ABP:{Avg_Price}:DI::1:PCE'",  # PRI segment for price details
            f"RFF+AAK:{Despatch_advice_number}'",  # RFF segment
            f"DTM+171:{Last_Receipt_Date if Last_Receipt_Date else ''}:102'",  # Additional DTM segment for the line item
        ]
        edifact_messages.append("\n".join(message))
        index = index + 1

    # Calculate the total number of segments
    total_segments = 8 + index * 6  # UNA, UNB, UNH, UNT, UNZ, and 6 segments per row

    # Create UNT and UNZ segments
    unt_segment = f"UNT+{total_segments}+{unique_ref}'"
    unz_segment = f"UNZ+1+{unique_ref}'"

    # Combine all messages with UNA, UNB, and UNH segments at the start
    edifact_data = (
        una_segment
        + "\n"
        + unb_unh_segments
        + "\n".join(edifact_messages)
        + "\n"
        + unt_segment
        + "\n"
        + unz_segment
    )
    return edifact_data


def main():
    try:
        connection = dbapi.connect(
            address="103.25.172.160",
            port=30015,
            database="RABYTE_PTE_LIVE",
            user="SYSTEM",
            password="Data1234",
        )
        if connection:
            print("🟢🟢 Connection Done !!")
            cursor = connection.cursor()
            query = """
                SELECT T1."CompnyName", T2."ItemCode", T2."ItemName", T2."U_SHIPITEM", 
                T2."CardCode",T3."Name" "Make", T4."Name" "Make+DIV", T2."OnHand", T2."BuyUnitMsr",
                T2."LastPurDat", T2."LstEvlPric" FROM "RABYTE_PTE_LIVE".OADM  T1, 
                "RABYTE_PTE_LIVE".OITM T2 INNER JOIN "RABYTE_PTE_LIVE"."@MAKE_FULL_1"  
                T3 ON T2."U_MF_1" = T3."Code" 
                INNER JOIN "RABYTE_PTE_LIVE"."@MAKE_FULL_2"  T4 ON T2."U_M_F_2" = T4."Code" 
                where T3."Name"='ams Osram' 
                LIMIT 5
            """
            result = cursor.execute(query)
            print("🟡🟡 Result fetched = ", result)

            # Transform the data into EDIFACT structure
            edifact_data = transform_to_edifact_structure(cursor)

            # Write the EDIFACT data to a file
            filename = (
                "Rabyte_sg"
                + datetime.now().strftime("%Y%m%d%H%M%S")
                + str(random.randint(10000000, 99999999))
            )
            reqfilepath = "output/"
            output_file = reqfilepath + filename + "_inventory_edi.txt"
            with open(output_file, "w") as f:
                f.write(edifact_data)

            print(f"EDIFACT message generated and saved to {output_file}")

    except:
        print("🔴🔴 Connection Unsuccessful!!")


if __name__ == "__main__":
    main()
