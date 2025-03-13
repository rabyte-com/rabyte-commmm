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
reqfolder = f"RE_report"

def format_date(date):
    d = str(date)
    d = d[:10]
    return d


query = f"""
        SELECT DISTINCT T1."DocEntry" AS "PO DocEntry",T1."DocStatus",T1."DocCur",Concat(T40."SeriesName",T1."DocNum"),T1."DocNum" AS "PO No.",T1."U_OrderNo" "SAGE PO NO",T1."U_UTL_Status" as "PO Document Approval Status",T1."CardCode",T1."CardName",T0."LineNum" as "PO Line No.",T0."VisOrder" "VisOrder",T0."LineStatus", T1."DocDate" as "PO Posting Date",T1."Comments" as "PO Remarks",T0."U_UTL_CPN" as "CPN (PO)",T0."U_UTL_PMAprvl",T0."U_UTL_OthAprvl",
        T0."ItemCode",T0."Dscription",Cast(T0."Text" as Varchar(20)) "Item Details",T0."U_FOC" "FOC Item",T0."U_FOCTyp" "FOC Type",T0."U_SAMPLETYPE" "SampleType",
        T0."FreeTxt",T0."U_Remarks2",T0."U_Remarks3",T0."U_Remarks4",T17."ChapterID" AS "HSN Code",T5."Name" as "Make",T15."Name" as "Make Full-2",T0."WhsCode",
        T0."Quantity" AS"Purchase Order Qty",
        --T12."U_UTL_AQTY" as "Commited Qty(Allocated Qty/MRN)",
        IFNULL(T2."Quantity",0) as "Received Qty",IFNULL(T0."OpenCreQty",0)"Balance Qty",
        --(IFNULL(T0."Quantity",0)-IFNULL(T2."Quantity",0)) as"Balance Qty",
        
        T0."U_CRDDate" AS "CRD (PO)",T4."LeadTime" AS "Lead Time in Days",
        T0."PriceBefDi" AS "Buy Price",T0."LineTotal" as "Purchase Amount",T4."OnHand" as "Total Stock",--T0."U_InStock" as "InStock",
        T11."DocNum" as "Sales Order No.",
        T12."LineNum" AS "SO Line No.",T12."VisOrder",T9."U_SOQTY" "SO Linked Qty",T12."U_CRDDate" as "CRD (SO)",T0."U_SuppEDD" as "Vendor EDD Date_PO",T12."U_SuppEDD" as "Vendor EDD Date_SO",T11."CardCode",T11."CardName",T11."NumAtCard" AS "Customer PO No.",T12."U_UTL_BPREF" AS "Customer PO Line No.",T24."ItmsGrpNam" "Category-1",
        T13."Name" AS "Category-2",T14."Name" as "Category-3",T20."Name" as "Category-4",T21."Name" as "Category-5",T19."LineNum" as "SQ LineNum",T22."DocNum" as "SQ DocNum",
        T15."U_PMName",T15."U_PMAName",T16."SlpName",T23."U_SALES_MANAGER",T0."U_UTL_RENESCMNO"
        
        
        FROM "RABYTE_PTE_LIVE".POR1 T0  
        
        INNER JOIN "RABYTE_PTE_LIVE".OPOR T1 ON T0."DocEntry" = T1."DocEntry" AND T1."CANCELED"='N'
        INNER  JOIN "RABYTE_PTE_LIVE".OITM T4 ON T0."ItemCode"=T4."ItemCode"
        LEFT JOIN "RABYTE_PTE_LIVE".OITB T24 ON T24."ItmsGrpCod"=T4."ItmsGrpCod"
        LEFT OUTER JOIN "RABYTE_PTE_LIVE".PDN1 T2 ON T0."DocEntry" = T2."BaseEntry" AND T0."LineNum" = T2."BaseLine" AND T2."ItemCode" = T0."ItemCode" 
        LEFT OUTER JOIN "RABYTE_PTE_LIVE".OPDN T3 ON T2."DocEntry"=T3."DocEntry" AND T3."CANCELED"='N' 
        LEFT OUTER JOIN "RABYTE_PTE_LIVE"."@MAKE_FULL_1" T5 ON T4."U_MF_1"= T5."Code"
        LEFT OUTER JOIN "RABYTE_PTE_LIVE"."@MAKE_FULL_2" T15 ON T4."U_M_F_2"=T15."Code"
        LEFT OUTER JOIN "RABYTE_PTE_LIVE"."@UTL_PODHED" T10 ON T0."DocEntry"=T10."U_BASEENTRY" AND T0."LineNum"=T10."U_BASELINE" 
        LEFT OUTER JOIN "RABYTE_PTE_LIVE"."@UTL_PODDET" T9 ON T10."DocEntry"=T9."DocEntry" AND T9."U_SOQTY">0
        LEFT OUTER JOIN "RABYTE_PTE_LIVE".OITW T8 ON T4."ItemCode"=T8."ItemCode" AND T8."ItemCode"=T0."ItemCode"
        LEFT OUTER JOIN "RABYTE_PTE_LIVE"."@UTL_SODHED" T6 ON T9."U_SODOCENTRY"=T6."U_BASEENTRY" AND T6."U_BASELINE"=T9."U_SOLINE" 
        LEFT OUTER JOIN "RABYTE_PTE_LIVE"."@UTL_SODDET" T7 ON T6."DocEntry"=T7."DocEntry" 
        LEFT OUTER JOIN "RABYTE_PTE_LIVE".RDR1 T12 ON T9."U_SOLINE"=T12."LineNum" AND T9."U_SODOCENTRY"=T12."DocEntry"
        LEFT OUTER JOIN "RABYTE_PTE_LIVE".ORDR T11 ON T12."DocEntry"=T11."DocEntry"
        LEFT OUTER JOIN "RABYTE_PTE_LIVE"."@CATEGORY_2" T13 ON T4."U_CAT2"=T13."Code"
        LEFT OUTER JOIN "RABYTE_PTE_LIVE"."@CATEGORY_3" T14 ON T4."U_CAT3"=T14."Code"
        LEFT OUTER JOIN "RABYTE_PTE_LIVE".OSLP T16 ON T11."SlpCode"=T16."SlpCode"
        LEFT JOIN "RABYTE_PTE_LIVE".OCRD T23 ON T23."CardCode"=T11."CardCode"
        LEFT OUTER JOIN "RABYTE_PTE_LIVE".OCHP T17 ON T4."ChapterID"=T17."AbsEntry"
        LEFT OUTER JOIN "RABYTE_PTE_LIVE"."@CATEGORY_4" T20 ON T4."U_CAT4"=T20."Code"
        LEFT OUTER JOIN "RABYTE_PTE_LIVE"."@CATEGORY_5" T21 ON T4."U_CAT5"=T21."Code"
        LEFT OUTER JOIN "RABYTE_PTE_LIVE".QUT1 T19 ON T12."BaseEntry"=T19."DocEntry" AND T12."BaseLine"=T19."LineNum"
        LEFT OUTER JOIN "RABYTE_PTE_LIVE".OQUT T22 ON T22."DocEntry"= T19."DocEntry"
        LEFT OUTER JOIN "RABYTE_PTE_LIVE".NNM1 T40 ON T40."Series"=T1."Series"
        
        WHERE 
        T0."U_UTL_PMAprvl"='A' 
        AND 
        T0."LineStatus"='O'
        AND
        T1."DocDate"='2025-02-14' 
        AND
        T5."Name"='Renesas'
        
        
        GROUP BY T1."DocNum", T1."DocDate", T1."CardCode",T1."Comments",T0."U_UTL_CPN",T0."ItemCode",T0."Dscription",Cast(T0."Text" as Varchar(20)),T5."Name",
        T1."DocEntry", T0."Quantity",T0."FreeTxt",T0."U_Remarks2",T0."U_Remarks3",T0."U_Remarks4",T17."ChapterID",T2."Quantity",T0."U_CRDDate",
        T4."LeadTime",T0."PriceBefDi" ,T0."LineTotal",T4."OnHand",T11."DocNum",T0."U_UTL_PMAprvl",T0."U_UTL_OthAprvl",T1."U_UTL_Status",
        T0."LineNum",T9."U_SOQTY",T9."U_SOLINE",T10."U_BASELINE",T12."LineNum",T12."Quantity",T12."ShipDate",T11."DocNum",T0."WhsCode",
        T1."CardName",T0."U_UTL_BPREF",T1."NumAtCard",T13."Name",T14."Name" ,T15."U_PMName",T15."U_PMAName",T0."U_InStock",T15."Name",
        T11."CardCode",T11."CardName",T11."NumAtCard",T12."U_UTL_BPREF",T12."U_CRDDate",T16."SlpName",T17."ChapterID",T0."FreeTxt",T0."LineStatus",
        T0."U_Remarks2",T0."U_Remarks3",T0."U_Remarks4" ,T20."Name",T21."Name",T19."LineNum",T22."DocNum",T12."U_UTL_AQTY",T12."U_SuppEDD",T1."DocStatus",T0."LineStatus",T40."SeriesName",
        T0."U_FOC" ,T0."U_FOCTyp" ,T0."U_SAMPLETYPE" ,T23."U_SALES_MANAGER",T24."ItmsGrpNam" ,T1."U_OrderNo",T1."DocCur",T0."VisOrder",T0."U_UTL_RENESCMNO",T0."U_SuppEDD",T0."OpenCreQty",T12."VisOrder"
        
        ORDER BY T1."DocNum",T0."LineNum"
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
            po_num = pos[4]
            # print(po_num, end=" ")
            line_data = {
                "po_date": pos[12],
                "po_num": pos[4],
                "qty": pos[31],
                "price" : pos[36],
                "mpn": pos[17],
                "p_name": pos[8],
                "crd": pos[43],
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
            po_date = format_date(po_details[0]['po_date'])
            file_name = (
                f"{reqfolder}/PO850_ISIL01_PO_{po_number}_{run_date}_{run_time}.txt"
            )
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
                file.write(f"BEG*00*SA*{po_number}**{po_date}]\n")
                file.write(f"N1*ST*<SOLD_TO>*92*<SOLD_ID>]\n")
                index = 1
                qty_sum = 0
                line_count = 5
                for data in po_item_dict[po_number]:
                    qty = data[31]
                    price = "%.3f" % data[36]
                    part_number = data[17]
                    partner_name = data[8][:30]
                    crd = format_date(data[43])
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
