from datetime import datetime
import os
import stat
import random
from hdbcli import dbapi

run_date = datetime.now().date().strftime("%d-%m-%Y")
date1 = datetime.now().date().strftime("%y%m%d")
now1 = datetime.now().strftime("%H%M")
run_time = datetime.now().strftime("%H_%M")
seq_no = 12534
date2 = datetime.now().date().strftime("%Y%m%d")
now2 = datetime.now().strftime("%H%M%S")
ref_no = random.randint(1000, 3000)

# create a connection with SAP LLP server for fetching data
reqfolder = f"POS_report"
file_name = f"{reqfolder}/POS_EDI_{run_date}.txt"
start_date = '20250303'
end_date = '20250309'

cnx2 = dbapi.connect(
    address="103.25.172.160",
    port=30015,
    database="RABYTE_LLP_LIVE01_0808",
    user="SYSTEM",
    password="Data1234",
)
print("🟢 LLP Connection Done")
query2 = f"""
        SELECT 'AR Invoice' "Type",

YEAR(oinv."DocDate") as "Year",

case when MONTH (oinv."DocDate") = '4' Then '1' 
when MONTH (oinv."DocDate") = '5' Then '2'
 when MONTH (oinv."DocDate") = '6' Then '3'
 when MONTH (oinv."DocDate") = '7' Then '4'
 when MONTH (oinv."DocDate") = '8' Then '5'
 when MONTH (oinv."DocDate") = '9' Then '6'
 when MONTH (oinv."DocDate") = '10' Then '7'
 when MONTH (oinv."DocDate") = '11' Then '8'
 when MONTH (oinv."DocDate") = '12' Then '9'
 when MONTH (oinv."DocDate") = '1' Then '10'
 when MONTH (oinv."DocDate") = '2' Then '11'
 when MONTH (oinv."DocDate") = '3' Then '12'
Else 0 end "Period"
,OINV."CardCode" as "BillToCustomer",

OINV."CardName" as "Name",

(SELECT concat(INV12."StreetNoB",INV12."BuildingB") FROM "RABYTE_LLP_LIVE01_0808".INV12 WHERE  INV12."DocEntry" =OINV."DocEntry") AS "AddressB1",
(SELECT concat(INV12."StreetB",INV12."BlockB") FROM "RABYTE_LLP_LIVE01_0808".INV12 WHERE  INV12."DocEntry" =OINV."DocEntry") AS "AddressB2",
(SELECT INV12."StateB" FROM "RABYTE_LLP_LIVE01_0808".INV12 WHERE  INV12."DocEntry" =OINV."DocEntry") AS "StateB",
(SELECT INV12."CountryB" FROM "RABYTE_LLP_LIVE01_0808".INV12 WHERE  INV12."DocEntry" =OINV."DocEntry") AS "CountryB",
(SELECT INV12."ZipCodeB" FROM "RABYTE_LLP_LIVE01_0808".INV12 WHERE  INV12."DocEntry" =OINV."DocEntry") AS "ZipCodeB",
(SELECT INV12."CityB" FROM "RABYTE_LLP_LIVE01_0808".INV12 WHERE  INV12."DocEntry" =OINV."DocEntry") AS "CityB",
(NNM1."SeriesName"||OINV."DocNum") AS "DocumentNo.",(OINV."DocDate") AS "Date",CASE WHEN INV1."VisOrder" = 0 THEN 1 ELSE INV1."VisOrder" + 1 END "LineItemNo",
  INV1."ItemCode" as "ItemCode",( CASE WHEN IFNULL(T5."DropShip",'N')='Y' THEN 0 ELSE IFNULL(INV1."Quantity",0) END) "Qty", 
INV1."unitMsr",CAST(INV1."PriceBefDi" as VARCHAR(200)) "SalesPrice",OINV."DocCur" AS "Currency",
INV1."StockPrice" as "PurchasePrice",INV1."U_UTL_BPREF" as "CustPO",T59."U_RENESCMNO" as "SCM Code",T2."U_COO" as "CompanyofOrgin(COO)",T52."Name" as "ABU/IIBU",
OINV."CardCode" as "ShipToCustomer",OINV."CardName" as "Name",

(SELECT concat(INV12."StreetNoS",INV12."BuildingS") FROM "RABYTE_LLP_LIVE01_0808".INV12 WHERE  INV12."DocEntry" =OINV."DocEntry") AS "AddressS1",
(SELECT concat(INV12."StreetS",INV12."BlockS") FROM "RABYTE_LLP_LIVE01_0808".INV12 WHERE  INV12."DocEntry" =OINV."DocEntry") AS "AddressS2",
(SELECT INV12."StateS" FROM "RABYTE_LLP_LIVE01_0808".INV12 WHERE  INV12."DocEntry" =OINV."DocEntry") AS "StateS",
(SELECT INV12."CountryS" FROM "RABYTE_LLP_LIVE01_0808".INV12 WHERE  INV12."DocEntry" =OINV."DocEntry") AS "CountryS",
(SELECT INV12."ZipCodeS" FROM "RABYTE_LLP_LIVE01_0808".INV12 WHERE  INV12."DocEntry" =OINV."DocEntry") AS "ZipCodeS",
(SELECT INV12."CityS" FROM "RABYTE_LLP_LIVE01_0808".INV12 WHERE  INV12."DocEntry" =OINV."DocEntry") AS "CityS",
 T59."U_RENESCMNO"  As "SCM Code",INV1."U_UTL_BPREF" AS "Quotation No",T59."U_OsramCustNo" AS "End Customer No",OINV."U_Project" AS "Project Name"
  
FROM "RABYTE_LLP_LIVE01_0808".OINV  
	 INNER JOIN "RABYTE_LLP_LIVE01_0808".INV1 ON INV1."DocEntry" =OINV."DocEntry"                   
	 LEFT OUTER join "RABYTE_LLP_LIVE01_0808".NNM1 on NNM1."Series" =OINV."Series" and IFNULL(OINV."Indicator",'') !='76'
	  INNER JOIN "RABYTE_LLP_LIVE01_0808".OITM T2 ON T2."ItemCode"=INV1."ItemCode"
	 LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OITB T3 ON T3."ItmsGrpCod"=T2."ItmsGrpCod"
	 LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".CRD1 T1 ON T1."CardCode"=OINV."CardCode" AND T1."Address"=OINV."ShipToCode" AND T1."AdresType"='S'
              LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OCRD T7 ON OINV."CardCode"=T7."CardCode"
	 LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OUSR T4  ON T4."USERID"=OINV."UserSign2"
	 LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OWHS T5 ON T5."WhsCode"=INV1."WhsCode"
              LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OCRG T6 ON T7."GroupCode"=T6."GroupCode"
              LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OSLP T9 ON OINV."SlpCode"=T9."SlpCode"
             LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808"."@MAKE_FULL_1" T51 ON T2."U_MF_1"= T51."Code"
            LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808"."@MAKE_FULL_2" T52 ON T2."U_M_F_2"=T52."Code"
             LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OSLP T56 ON T7."SlpCode"=T56."SlpCode"
             LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".DLN1 CC ON INV1."BaseEntry" = CC."DocEntry" AND INV1."BaseLine" = CC."LineNum" 
             LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".ODLN BB ON BB."DocEntry" = CC."DocEntry" 

            LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".RDR1 T57 ON CC."BaseEntry" = T57."DocEntry" AND CC."BaseLine" = T57."LineNum" AND T57."TargetType" = 15 OR (T57."TargetType"= 13 AND INV1."BaseEntry" = T57."DocEntry" AND INV1."BaseLine" = T57."LineNum") 
              LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".ORDR T58 ON T57."DocEntry"=T58."DocEntry"
              LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OCRD T59 ON T59."CardCode"=OINV."CardCode"

WHERE OINV."CANCELED" in ('N') AND OINV."DocDate" BETWEEN '2025-03-03 00:00:01' AND '2025-03-09 23:59:59'
GROUP BY OINV."TransId",OINV."CardCode",OINV."CardName",
OINV."DocDate",OINV."DocEntry",OINV."ObjType",NNM1."SeriesName",OINV."DocNum",OINV."NumAtCard",OINV."DiscPrcnt",OINV."DiscSum",
OINV."DocTotal",OINV."RoundDif",OINV."Project",INV1."LocCode",OINV."WTSum",INV1."VisOrder",INV1."ItemCode",INV1."Dscription",
INV1."Quantity",INV1."LineTotal",INV1."AcctCode",INV1."LineNum", T3."ItmsGrpNam", T2."SalUnitMsr",INV1."Price",INV1."DiscPrcnt",T1."GSTRegnNo",T1."GSTType",T7."U_SALES_MANAGER",
OINV."GSTTranTyp",T2."GstTaxCtg",T4."U_NAME",
OINV."CreateDate",OINV."DocCur",INV1."DocEntry",T5."DropShip",
 T6."GroupName",INV1."TaxCode",OINV."DocEntry",OINV."TotalExpns",
 T52."U_PMName",T58."DocNum",T9."SlpName",T51."Name" ,
 T52."Name",T56."SlpName",T7."OwnerCode" ,T7."CardFName" ,
 INV1."LineNum",T7."U_RENESCMNO",OINV."DocTime",INV1."U_UTL_BPREF",INV1."PriceBefDi",
 INV1."Currency",INV1."VatSum",INV1."VatSumFrgn",T57."U_CRDDate",INV1."NoInvtryMv",
 INV1."U_Flag",INV1."U_UTL_CPN",T57."U_UTL_CPN",INV1."unitMsr",T2."U_COO",INV1."StockPrice",INV1."U_UTL_BPREF",T59."U_RENESCMNO",T59."U_OsramCustNo",OINV."U_Project" 


UNION ALL 



SELECT 'AR Credit Invoice' "Type",
YEAR(orin."DocDate") as "Year",
case when MONTH (orin."DocDate") = '4' Then '1' 
when MONTH (orin."DocDate") = '5' Then '2'
 when MONTH (orin."DocDate") = '6' Then '3'
 when MONTH (orin."DocDate") = '7' Then '4'
 when MONTH (orin."DocDate") = '8' Then '5'
 when MONTH (orin."DocDate") = '9' Then '6'
 when MONTH (orin."DocDate") = '10' Then '7'
 when MONTH (orin."DocDate") = '11' Then '8'
 when MONTH (orin."DocDate") = '12' Then '9'
 when MONTH (orin."DocDate") = '1' Then '10'
 when MONTH (orin."DocDate") = '2' Then '11'
 when MONTH (orin."DocDate") = '3' Then '12'
Else 0 end "Period",

ORIN."CardCode" as "Customer",
ORIN."CardName" as "Name",

(SELECT concat(RIN12."StreetNoB",RIN12."BuildingB") FROM "RABYTE_LLP_LIVE01_0808".RIN12 WHERE  RIN12."DocEntry" =ORIN."DocEntry") AS "AddressB1",
(SELECT concat(RIN12."StreetB",RIN12."BlockB") FROM "RABYTE_LLP_LIVE01_0808".RIN12 WHERE  RIN12."DocEntry" =ORIN."DocEntry") AS "AddressB2",
(SELECT RIN12."StateB" FROM "RABYTE_LLP_LIVE01_0808".RIN12 WHERE  RIN12."DocEntry" =ORIN."DocEntry") AS "StateB",
(SELECT RIN12."CountryB" FROM "RABYTE_LLP_LIVE01_0808".RIN12 WHERE  RIN12."DocEntry" =ORIN."DocEntry") AS "CountryB",
(SELECT RIN12."ZipCodeB" FROM "RABYTE_LLP_LIVE01_0808".RIN12 WHERE  RIN12."DocEntry" =ORIN."DocEntry") AS "ZipCodeB",
(SELECT RIN12."CityB" FROM "RABYTE_LLP_LIVE01_0808".RIN12 WHERE  RIN12."DocEntry" =ORIN."DocEntry") AS "CityB",
(NNM1."SeriesName"||ORIN."DocNum")as "DocumentNo",
(ORIN."DocDate") AS "Date",
CASE WHEN RIN1."VisOrder" = 0 THEN 1 ELSE RIN1."VisOrder" + 1 END "LineItemNo",

  RIN1."ItemCode" as "Item Code",
  -( CASE WHEN IFNULL(T5."DropShip",'N')='Y' OR RIN1."NoInvtryMv"='Y' THEN 0 ELSE IFNULL(RIN1."Quantity",0) END) "Qty", 
   RIN1."unitMsr",CAST(-RIN1."PriceBefDi" as VARCHAR(200)) "SalesPrice",ORIN."DocCur" AS "Currency",
   RIN1."StockPrice" as "PurchasePrice",RIN1."U_UTL_BPREF" as "CustPO",T59."U_RENESCMNO" "SCM Code",T2."U_COO" as "CompanyofOrgin(COO)",T52."Name" as "ABU/IIBU",
 

ORIN."CardCode" as "ShipToCustomer",
ORIN."CardName" as "Name",


(SELECT concat(RIN12."StreetNoS",RIN12."BuildingS") FROM "RABYTE_LLP_LIVE01_0808".RIN12 WHERE  RIN12."DocEntry" =ORIN."DocEntry") AS "AddressS1",
(SELECT concat(RIN12."StreetS",RIN12."BlockS") FROM "RABYTE_LLP_LIVE01_0808".RIN12 WHERE  RIN12."DocEntry" =ORIN."DocEntry") AS "AddressS2",
(SELECT RIN12."StateS" FROM "RABYTE_LLP_LIVE01_0808".RIN12 WHERE  RIN12."DocEntry" =ORIN."DocEntry") AS "StateS",
(SELECT RIN12."CountryS" FROM "RABYTE_LLP_LIVE01_0808".RIN12 WHERE  RIN12."DocEntry" =ORIN."DocEntry") AS "CountryS",
(SELECT RIN12."ZipCodeS" FROM "RABYTE_LLP_LIVE01_0808".RIN12 WHERE  RIN12."DocEntry" =ORIN."DocEntry") AS "ZipCodeS",
(SELECT RIN12."CityS" FROM "RABYTE_LLP_LIVE01_0808".RIN12 WHERE  RIN12."DocEntry" =ORIN."DocEntry") AS "CityS",
T59."U_RENESCMNO" "SCM Code",RIN1."U_UTL_BPREF" AS "Quotation No",T59."U_OsramCustNo" AS "End Customer No",ORIN."U_Project" AS "Project Name"
FROM "RABYTE_LLP_LIVE01_0808".ORIN  
	 INNER JOIN "RABYTE_LLP_LIVE01_0808".RIN1 ON RIN1."DocEntry" =ORIN."DocEntry"                   
	 LEFT OUTER join "RABYTE_LLP_LIVE01_0808".NNM1 on NNM1."Series" =ORIN."Series" and IFNULL(ORIN."Indicator",'') !='76'
	INNER JOIN "RABYTE_LLP_LIVE01_0808".OITM T2 ON T2."ItemCode"=RIN1."ItemCode"
	 LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OITB T3 ON T3."ItmsGrpCod"=T2."ItmsGrpCod"
	 LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".CRD1 T1 ON T1."CardCode"=ORIN."CardCode" AND T1."Address"=ORIN."ShipToCode" AND T1."AdresType"='S'
              LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OCRD T7 ON ORIN."CardCode"=T7."CardCode"
	 LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OUSR T4  ON T4."USERID"=ORIN."UserSign2"
	 LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OWHS T5 ON T5."WhsCode"=RIN1."WhsCode"
             LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OCRG T6 ON T7."GroupCode"=T6."GroupCode"
             LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OSLP T9 ON ORIN."SlpCode"=T9."SlpCode"
             LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808"."@MAKE_FULL_1" T51 ON T2."U_MF_1"= T51."Code"
            LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808"."@MAKE_FULL_2" T52 ON T2."U_M_F_2"=T52."Code"
            LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OSLP T56 ON T7."SlpCode"=T56."SlpCode"
            LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".RDR1 T57 ON T57."DocEntry"=RIN1."BaseEntry" AND RIN1."BaseLine"=T57."LineNum"
            LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".ORDR T58 ON T57."DocEntry"=T58."DocEntry"
 	   LEFT OUTER JOIN "RABYTE_LLP_LIVE01_0808".OCRD T59 ON T59."CardCode"=ORIN."CardCode"


WHERE ORIN."CANCELED"='N' AND ORIN."DocDate" BETWEEN '2025-03-03 00:00:01' AND '2025-03-09 23:59:59'
GROUP BY ORIN."TransId",ORIN."CardCode",ORIN."CardName",
ORIN."DocDate",ORIN."DocEntry",T52."U_PMName",ORIN."ObjType",RIN1."VisOrder",NNM1."SeriesName",ORIN."DocNum",ORIN."NumAtCard",ORIN."DiscPrcnt",ORIN."DiscSum",
ORIN."DocTotal",ORIN."RoundDif",ORIN."Project",RIN1."LocCode",ORIN."WTSum",RIN1."ItemCode",RIN1."Dscription",
RIN1."Quantity",RIN1."LineTotal",RIN1."AcctCode",RIN1."LineNum", T3."ItmsGrpNam", T2."SalUnitMsr",RIN1."Price",RIN1."DiscPrcnt",T1."GSTRegnNo",T1."GSTType",
ORIN."GSTTranTyp",T2."GstTaxCtg",T4."U_NAME",ORIN."CreateDate",T58."DocNum",T7."U_SALES_MANAGER",ORIN."DocCur",RIN1."DocEntry",T5."DropShip",T6."GroupName",ORIN."DocEntry",ORIN."TotalExpns", RIN1."TaxCode",T9."SlpName",T51."Name" ,T57."U_CRDDate",RIN1."VatSum",RIN1."VatSumFrgn",RIN1."Currency",RIN1."PriceBefDi",T52."Name",T7."CardFName",T56."SlpName",T7."OwnerCode",RIN1."LineNum",T7."U_RENESCMNO",ORIN."DocTime",RIN1."U_UTL_BPREF", RIN1."NoInvtryMv",RIN1."U_Flag",
RIN1."U_UTL_CPN",T57."U_UTL_CPN",RIN1."unitMsr",T2."U_COO",RIN1."StockPrice",RIN1."U_UTL_BPREF",T59."U_RENESCMNO",
RIN1."U_UTL_BPREF",T59."U_OsramCustNo",ORIN."U_Project";

    """

cursor2 = cnx2.cursor()
cursor2.execute(query2)
data = cursor2.fetchall()

po_dict = {}
po_item_dict = {}

if len(data) == 0:
    print("🔴🔴 No data available")
else:
    print("✅✅ Data fetched ✅✅\n")
    for pos in data:
        invoice_no = pos[12]        
        line_data = {
            "int_cust_code" : pos[3],
            "int_cust_name" : pos[4],
            "int_street" : pos[6],
            "int_postal" : pos[10],
            "int_country" : pos[9],
            "item_code" : pos[15],
            "qty" : pos[16],
            "currency" : pos[18]
            }

        if invoice_no not in po_dict:
            po_dict[invoice_no] = []

        if invoice_no not in po_item_dict:
            po_item_dict[invoice_no] = []

        if line_data not in po_dict[invoice_no]:
            po_dict[invoice_no].append(line_data) 

        po_item_dict[invoice_no].append(data)

        if not os.path.exists(reqfolder):
            os.makedirs(reqfolder)
            os.chmod(reqfolder, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

        for invoice_no, line_items in po_dict.items():
            pos_details = line_items
            cux = pos_details[0]['currency']
            cust_code = pos_details[0]['int_cust_code']
            cust_name = pos_details[0]['int_cust_name'] 
            street = pos_details[0]['int_street']
            post_code = pos_details[0]["int_postal"]
            country = pos_details[0]["int_country"]
            with open(file_name, "w") as file:
                file.write(f"UNA:+.? '\n")
                file.write(f"UNB+UNOC:3+RabyteIN:ZZ+OSRAMOSPENTEST+{date1}:{now1}+{ref_no}++SLSRPT'\n")
                file.write(f"UNH+{ref_no}00001+SLSRPT:D:97A:ED:EDSR03'\n")
                file.write(f"BGM+RES+{date2}{now2}+9'\n")
                file.write(f"DTM+137:{date2}:102'\n")
                file.write(f"DTM+90:{start_date}:102'\n")
                file.write(f"DTM+91:{end_date}:102'\n")
                file.write(f"NAD+BY+Rabyte Technology::92'\n")
                file.write(f"NAD+DB+:520252:91'\n")
                file.write(f"CUX+2:{cux}:4'\n")
                file.write(f"CUX+2:{cux}:9'\n")
                file.write(f"DOC+380'\n")
                file.write(
                    f"NAD+PC+{cust_code}::92++{cust_name}+{street}++{post_code}+{country}'\n"
                )
                index = 1
                for data in po_item_dict[invoice_no]:
                    item_code = data[index-1][15]
                    qty = data[index-1][16]
                    res_price = data[index-1][20]
                    doc_num = data[index-1][12]
                    inv_date = data[index-1][13]
                    file.write(f"LIN+{index}+114+{item_code}:VP::91'\n")
                    file.write(f"QTY+1+{qty}:PCE'\n")
                    file.write(f"PRI+AAA::DIS::1:PCE'\n")
                    file.write(f"PRI+AAA:{res_price}:RES::1:PCE'\n")
                    file.write(f"RFF+DM:{doc_num}:041'\n")
                    file.write(f"DTM+171:{inv_date}:102'\n")
                    file.write(f"RFF+PR:METER'\n")
                    file.write(f"RFF+PL:'\n")
                    file.write(f"RFF+DC:DC'\n")
                    index += 1
                # file.write(f"NAD+MA+<END_CUSTOMER_CODE>::92++<END_CUSTOMER_NAME>+<STREET>+<CITY>+++<COUNTRY_CODE>'\n")
                file.write(f"UNT+4+{seq_no}00001'\n")
                file.write(f"UNZ+1+{seq_no}'\n")
    print("✔✔ Completed")
