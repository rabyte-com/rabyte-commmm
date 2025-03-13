from hdbcli import dbapi

connection = dbapi.connect(
    address="103.25.172.160",
    port=30015,
    database="RABYTE_PTE_LIVE",
    user="SYSTEM",
    password="Data1234",
)

sql4 = """
        SELECT T1."CompnyName", T2."ItemCode", T2."ItemName", T2."U_SHIPITEM", 
        T2."CardCode",T3."Name" "Make", T4."Name" "Make+DIV", T2."OnHand", T2."BuyUnitMsr",
        T2."LastPurDat", T2."LstEvlPric" FROM "RABYTE_PTE_LIVE".OADM  T1, 
        "RABYTE_PTE_LIVE".OITM T2 INNER JOIN "RABYTE_PTE_LIVE"."@MAKE_FULL_1"  
        T3 ON T2."U_MF_1" = T3."Code" 
        INNER JOIN "RABYTE_PTE_LIVE"."@MAKE_FULL_2"  T4 ON T2."U_M_F_2" = T4."Code" 
        where T3."Name"='ams Osram' LIMIT 10
    """
cursor = connection.cursor()
result = cursor.execute(sql4)
print(cursor)
print(result)

for data in cursor:
    print(data)