"""Python Script to fecth data from LLP server for PO Osram EDI"""

from hdbcli import dbapi
from datetime import datetime
import os
import stat

# Get the current date and datetime
# current_date = datetime.now().date()
current_date = datetime(2024, 2, 26)
now = datetime.now()
current_time = now.replace(microsecond=0).time()
time_str = str(current_time).replace(":", "").split(".")[0]

# Format the date as ddmmyy
formatted_date = current_date.strftime("%d%m%y")

# create folder structure
reqfolder = f"in_{formatted_date}"


# format the date for use
def format_date(date_str):
    # Parse the date string
    date_string = str(date_str)
    date_object = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")

    # Format the date to ddmmyy
    formatted_date = date_object.strftime("%y%m%d")

    # Calculate the century
    century = date_object.year // 100
    cc = f"{century:02d}"

    # Combine cc and formatted_date
    result = f"{cc}{formatted_date}"

    return result


# create a connection with SAP LLP server for fetching data
connection = dbapi.connect(
    address="103.25.172.160",
    port=30015,
    database="RABYTE_LLP_LIVE01_0808",
    user="SYSTEM",
    password="Data1234",
)

# sql query to fetch required data from database
query = f"""
        SELECT DISTINCT T1."DocNum" AS "Purchase order number",T1."DocDate" as "Purchase order date",T1."NumAtCard" "Contract/Quote number/Quote Line Item",

        T11."CardCode" "Buyer/End Customer code/Ship To",'' "Assigned by seller/Buyer",'' "Party Name - STREET/City/Region/postalcode/country",

        (SELECT PY."PymntGroup" FROM "RABYTE_LLP_LIVE01_0808".OCTG PY WHERE PY."GroupNum" = T1."GroupNum")"Payment Terms",T1."U_Price" AS "Delivery Term",

        '' "LINE ITEM No",T0."ItemCode" as "Supplier’s Part Number",T0."ItemCode" as "Vendor's part number",'' as "Assigned by seller/Buyer",

        T0."Dscription"  as "Customer Part No",T0."ItemCode" as "Distributor’s Part Number",

        T0."Quantity" AS "Ordered quantity",T0."Quantity" AS "Ordered total quantity",T0."PriceBefDi" AS "Distributor Unit price",

        '' as "Buyer's original line item Number",'' as "Contract/Quote number",'' as "Buyer's original line item number",

        '' as "SCHEDULING CONDITIONS",'' as "Ordered quantity",'' as "Schedule QTY",

        T0."U_CRDDate" as "Buyer Delivery date/time",T12."U_UTL_BPREF" AS "Contract/Quote Ref if applicable at HEADER level",'' as "Buyer Code",

        T1."U_OSRAMEndCustNo" as "Disty Code given by supplier (End Customer Code)",T41."Currency",

        T0."ItemCode" as "Vendor Part No",''  as "Customer Part No",'PCS' as "Price And Unit Basic",

        '' as "Customer Line Item No",T1."DocStatus" "PO Status",T11."DocStatus" as "So Status",T1."NumAtCard" as "QuotaionNo",T1."DocCur",T0."U_OsramQuoteNo",T0."U_OsramLineNo"

        FROM "RABYTE_LLP_LIVE01_0808".POR1 T0  

        INNER JOIN "RABYTE_LLP_LIVE01_0808".OPOR T1 ON T0."DocEntry" = T1."DocEntry" AND T1."CANCELED"='N'

        INNER  JOIN "RABYTE_LLP_LIVE01_0808".OITM T4 ON T0."ItemCode"=T4."ItemCode"

        LEFT JOIN "RABYTE_LLP_LIVE01_0808".OITB T24 ON T24."ItmsGrpCod"=T4."ItmsGrpCod"

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".PDN1 T2 ON T0."DocEntry" = T2."BaseEntry" AND T0."LineNum" = T2."BaseLine" AND T2."ItemCode" = T0."ItemCode" 

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OPDN T3 ON T2."DocEntry"=T3."DocEntry" AND T3."CANCELED"='N' 

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808"."@MAKE_FULL_1" T5 ON T4."U_MF_1"= T5."Code"

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808"."@MAKE_FULL_2" T15 ON T4."U_M_F_2"=T15."Code"

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808"."@UTL_PODHED" T10 ON T0."DocEntry"=T10."U_BASEENTRY" AND T0."LineNum"=T10."U_BASELINE" 

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808"."@UTL_PODDET" T9 ON T10."DocEntry"=T9."DocEntry" AND T9."U_SOQTY">0

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OITW T8 ON T4."ItemCode"=T8."ItemCode" AND T8."ItemCode"=T0."ItemCode"

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808"."@UTL_SODHED" T6 ON T9."U_SODOCENTRY"=T6."U_BASEENTRY" AND T6."U_BASELINE"=T9."U_SOLINE" 

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808"."@UTL_SODDET" T7 ON T6."DocEntry"=T7."DocEntry" 

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".RDR1 T12 ON T9."U_SOLINE"=T12."LineNum" AND T9."U_SODOCENTRY"=T12."DocEntry"

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".ORDR T11 ON T12."DocEntry"=T11."DocEntry"

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808"."@CATEGORY_2" T13 ON T4."U_CAT2"=T13."Code"

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808"."@CATEGORY_3" T14 ON T4."U_CAT3"=T14."Code"

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OSLP T16 ON T11."SlpCode"=T16."SlpCode"

        LEFT JOIN "RABYTE_LLP_LIVE01_0808".OCRD T23 ON T23."CardCode"=T11."CardCode"

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OCHP T17 ON T4."ChapterID"=T17."AbsEntry"

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808"."@CATEGORY_4" T20 ON T4."U_CAT4"=T20."Code"

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808"."@CATEGORY_5" T21 ON T4."U_CAT5"=T21."Code"

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".QUT1 T19 ON T12."BaseEntry"=T19."DocEntry" AND T12."BaseLine"=T19."LineNum"

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OQUT T22 ON T22."DocEntry"= T19."DocEntry"

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".NNM1 T40 ON T40."Series"=T1."Series"

        LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OCRD T41 ON T41."CardCode"=T11."CardCode"

        where  T1."U_MGMTDate"='{current_date}' and T5."Name"='ams Osram' and T0."LineStatus"='O' and T1."U_MGMTAPPROVAL"='Yes'

        GROUP BY T1."DocNum", T1."DocDate", T1."CardCode",T1."Comments",T0."U_UTL_CPN",T0."ItemCode",T0."Dscription",Cast(T0."Text" as Varchar(20)),T5."Name",

        T1."DocEntry", T0."Quantity",T0."FreeTxt",T0."U_Remarks2",T0."U_Remarks3",T0."U_Remarks4",T17."ChapterID",T2."Quantity",T0."U_CRDDate",

        T4."LeadTime",T0."PriceBefDi" ,T0."LineTotal",T4."OnHand",T11."DocNum",T0."U_UTL_PMAprvl",T0."U_UTL_OthAprvl",T1."U_UTL_Status",

        T0."LineNum",T9."U_SOQTY",T9."U_SOLINE",T10."U_BASELINE",T12."LineNum",T12."Quantity",T12."ShipDate",T11."DocNum",T0."WhsCode",

        T1."CardName",T0."U_UTL_BPREF",T1."NumAtCard",T13."Name",T14."Name" ,T15."U_PMName",T15."U_PMAName",T0."U_InStock",T15."Name",

        T11."CardCode",T11."CardName",T11."NumAtCard",T12."U_UTL_BPREF",T12."U_CRDDate",T16."SlpName",T17."ChapterID",T0."FreeTxt",T0."LineStatus",T9."U_AQTY",

        T0."U_Remarks2",T0."U_Remarks3",T0."U_Remarks4" ,T20."Name",T21."Name",T19."LineNum",T22."DocNum",T12."U_UTL_AQTY",T12."U_SuppEDD",T1."DocStatus",T0."LineStatus",T40."SeriesName",

        T0."U_FOC" ,T0."U_FOCTyp" ,T0."U_SAMPLETYPE" ,T23."U_SALES_MANAGER",

        T24."ItmsGrpNam" ,T1."U_OrderNo",T1."DocCur",T0."VisOrder",

        T0."U_UTL_RENESCMNO",T0."U_SuppEDD",T0."OpenCreQty",T12."VisOrder",T11."Address2",T1."GroupNum",T1."U_Price"

        ,T41."Currency",T41."U_OsramCustNo",T1."DocStatus",T11."DocStatus",T12."U_UTL_CPN",T1."U_OSRAMEndCustNo",T1."NumAtCard",T1."DocCur",T0."U_OsramQuoteNo",T0."U_OsramLineNo"

        ORDER BY T1."DocNum",T0."LineNum"   
        """

# create connection with the database
cursor = connection.cursor()
result = cursor.execute(query)

# Fetch the column headers
column_names = [desc[0] for desc in cursor.description]

# create two dictionary to store fetched data
po_dict = {}
po_item_dict = {}

# fetch all the data
data = cursor.fetchall()

# check for data in database
if len(data) == 0:
    # if no data available, exit out
    print("🔴🔴 No data available yet 🔴🔴")
else:
    print("✅✅ Data fetched ✅✅\n")
    print("Available PO Number: ")
    for data in cursor.fetchall():
        po_number = data[0]
        print(po_number, end=" ")
        date_object = data[1]
        line_data = {
            "po_date": format_date(date_object),
            "po_number": data[0],
            "quote_number": data[2],
            "buyer": data[3],
            "payment_terms": data[6],
            "end_cust_code": data[26],
            "curr": data[35],
        }

        if po_number not in po_dict:
            po_dict[po_number] = []

        if po_number not in po_item_dict:
            po_item_dict[po_number] = []

        if line_data not in po_dict[po_number]:
            po_dict[po_number].append(line_data)

        po_item_dict[po_number].append(data)

    # close the connection
    cursor.close()

    # check for the output folder
    if not os.path.exists(reqfolder):
        # create the folder
        os.makedirs(reqfolder)
        # change folder permissions to 777
        os.chmod(reqfolder, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    # write the fetched data into the desired format provided by osram into a text file
    for po_number, line_items in po_dict.items():
        po_details = line_items
        formatted_date1 = current_date.strftime("%d%m%y")
        reqdate = current_date.strftime("%d%m%y")
        hour_minute = str(now.time()).split(":")[:2]
        hour_minute = "".join(hour_minute)
        reqtime = hour_minute
        po_numbers1 = str(po_number) + str(formatted_date1) + str(time_str)
        po_date = po_details[0]["po_date"]
        curr = po_details[0]["curr"]
        file_name = f"{reqfolder}/RabyteIN_{po_number}.txt"

        with open(file_name, "w") as file:
            file.write("UNA:+.? '\n")
            file.write(
                f"UNB+UNOA:2+RabyteIN:zz+OSRAMOSPEN+{reqdate}:{reqtime}+{po_numbers1}++ORDERS'\n"
            )
            file.write(f"UNH+{po_number}+ORDERS:D:97A:UN'\n")
            file.write(f"BGM+220+{po_number}+9'\n")
            file.write(f"DTM+137:{po_date}:102'\n")
            file.write("NAD+BY+520252::91'\n")
            file.write("NAD+ST+VIMP00281'\n")
            file.write(
                f"NAD+Z6+{po_details[0]['end_cust_code'] if po_details[0]['end_cust_code'] else ''}::92'\n"
            )
            file.write(f"CUX+2:{curr}:9'\n")
            file.write(f"PAT+ZZZ+6:::{po_details[0]['payment_terms']}'\n")
            file.write("TOD+6++:::FCA'\n")
            rqlno = 0
            for data2 in po_item_dict[po_number]:
                itemdesc = data2[12]
                quoteNumber = data2[36]
                qlineno = data2[37]

                file.write(f"LIN+{rqlno}++{data2[13]}:VP::91'\n")
                file.write(f"PIA+1+{itemdesc[:32] if itemdesc else ''}:BP::92'\n")
                file.write(f"QTY+21:{data2[14]}:PCE'\n")
                file.write(f"PRI+AAA:{data2[16]}:DI::1:PCE'\n")
                file.write(
                    f"RFF+CT:{quoteNumber if quoteNumber else ''}:{qlineno if qlineno else ''}'\n"
                )
                file.write(f"RFF+LI::{rqlno}'\n")
                file.write("SCC+1'\n")
                file.write(f"QTY+21:{data2[14] if data2[14] else ''}'\n")
                file.write(
                    f"DTM+2:{format_date(data2[23]) if data2[23] else ''}:102'\n"
                )
                rqlno = rqlno + 1

            file.write("UNS+S'\n")
            file.write(f"UNT+8+{po_number}'\n")
            file.write(f"UNZ+1+{po_numbers1}'\n")

    print("\n🟢🟢 Data Transfer Complete 🟢🟢")

""" End of Script """
