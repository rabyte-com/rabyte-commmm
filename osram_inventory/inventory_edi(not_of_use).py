import pandas as pd
from datetime import datetime
import mysql.connector
import random

#def format_date(date_str):
 #   date_str = str(date_str)  # Ensure the date is treated as a string
  #  return f"{date_str[:4]}{date_str[4:6]}{date_str[6:]}"
def format_date(date_str):
        # Parse the date string
    date_string = str(date_str)    
    date_object = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')

    # Format the date to ddmmyy
    formatted_date = date_object.strftime('%y%m%d')

    # Calculate the century
    century = date_object.year // 100
    cc = f"{century:02d}"

    # Combine cc and formatted_date
    result = f"{cc}{formatted_date}"
    
    return result

def transform_to_edifact_structure(df):
    edifact_messages = []

    # Add UNA segment for service string advice
    una_segment = "UNA:+.? '"

    # Generate current date and time for UNB segment
    current_datetime = datetime.now()
    date_of_preparation = current_datetime.strftime('%y%m%d')
    time_of_preparation = current_datetime.strftime('%H%M')
    #unique_ref1 = datetime.now().strftime('%Y%m%d%H%M%S')+ str(random.randint(10000, 9999))
    unique_ref = datetime.now().strftime('%y%m%d')+ str(random.randint(1000, 9999))
    unique_ref1 =datetime.now().strftime('%Y%m%d%H%M%S')
    partnermailbox="RabyteIN"
    dbid="520252" # NAD segment for distributor (static example)
    dbname="Rabyte IN" # NAD segment for buyer (static example)
    cux = "INR" # CUX segment for currency details (static example)
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
    for index, row in df.iterrows():
        message = [
            f"LIN+{index+1}++{row['ItemNumber']}:VP::91'",  # LIN segment for line item
            f"INV++++2'",  # INV segment for inventory management details
            f"QTY+17:{row['Quantity']}:PCE'",  # QTY segment for quantity
            f"PRI+ABP:{row['Price']}:DI::1:PCE'",  # PRI segment for price details
            f"RFF+AAK:{Despatch_advice_number}'",  # RFF segment
            f"DTM+171:{format_date(row['LastReceiptDate']) if row['LastReceiptDate'] else ''}:102'"  # Additional DTM segment for the line item
        ]
        edifact_messages.append("\n".join(message))

    # Calculate the total number of segments
    total_segments = 8 + len(df) * 6  # UNA, UNB, UNH, UNT, UNZ, and 6 segments per row

    # Create UNT and UNZ segments
    unt_segment = f"UNT+{total_segments}+{unique_ref}'"
    unz_segment = f"UNZ+1+{unique_ref}'"

    # Combine all messages with UNA, UNB, and UNH segments at the start
    edifact_data = una_segment + "\n" + unb_unh_segments + "\n".join(edifact_messages) + "\n" + unt_segment + "\n" + unz_segment
    return edifact_data

def main():
    # MySQL database connection details
    user = 'admin'
    password = 'Admin@123'
    host = '127.0.0.1'
    database = 'make_inventry'
    
    # Connect to the MySQL database
    connection = mysql.connector.connect(
        user=user,
        password=password,
        host=host,
        database=database
    )
    
    query = """
    SELECT  
        Material_Code as ItemNumber,
        Stock_Qty as Quantity,
        Avg_Price as Price,
        Last_Receipt_Date as LastReceiptDate,
        Make  
    FROM mst_inventory 
    WHERE Stock_Qty > 0 and Avg_Price >0 AND Make LIKE 'ams%' limit 2
    """
    
    # Load the data into a DataFrame
    df = pd.read_sql(query, connection)
    
    # Close the connection
    connection.close()
    
    # Transform the data into EDIFACT structure
    edifact_data = transform_to_edifact_structure(df)
    
    # Write the EDIFACT data to a file
    filename='Rabyte_in'+datetime.now().strftime('%Y%m%d%H%M%S')+ str(random.randint(10000000, 99999999))
    reqfilepath = "/home/santosh/ed1/edifile/osram/"
    output_file = reqfilepath+filename+'_inventory_edi.txt'
    with open(output_file, 'w') as f:
        f.write(edifact_data)
    
    print(f'EDIFACT message generated and saved to {output_file}')

if __name__ == "__main__":
    main()

